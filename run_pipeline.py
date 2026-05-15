from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import webbrowser
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import gspread
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import URL


DEFAULT_TABLE = "financial_transactions"


def log_step(message: str) -> None:
    print(f"[PIPELINE] {message}", flush=True)


def build_mysql_engine(
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
) -> Engine:
    connection_uri = URL.create(
        "mysql+mysqlconnector",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    return create_engine(
        connection_uri,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def load_input_dataset(input_path: Path) -> pd.DataFrame:
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(input_path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_path)
    raise ValueError(
        f"Unsupported dataset format '{suffix}'. Use .csv, .xlsx, or .xls."
    )


def detect_date_columns(columns: Iterable[str]) -> List[str]:
    date_markers = ("date", "time", "timestamp")
    return [col for col in columns if any(marker in col.lower() for marker in date_markers)]


def detect_amount_column(df: pd.DataFrame) -> str:
    preferred = ["amount", "transaction_amount", "value", "total"]
    normalized = {c.lower(): c for c in df.columns}
    for key in preferred:
        if key in normalized:
            return normalized[key]

    numeric_candidates = list(df.select_dtypes(include=[np.number]).columns)
    if not numeric_candidates:
        raise ValueError(
            "No numeric amount-like column found. Please include an amount column."
        )
    return numeric_candidates[0]


def normalize_transaction_flow(
    row: pd.Series, amount_col: str, type_col: Optional[str]
) -> str:
    raw_type = str(row[type_col]).lower().strip() if type_col else ""
    amount = float(row[amount_col])

    inflow_markers = ("revenue", "income", "sale", "cash_in", "credit", "inflow")
    outflow_markers = ("expense", "cost", "cogs", "opex", "cash_out", "debit", "outflow")

    if any(token in raw_type for token in inflow_markers):
        return "Revenue"
    if any(token in raw_type for token in outflow_markers):
        return "Expense"
    return "Revenue" if amount >= 0 else "Expense"


def categorize_transaction(description: str, flow: str) -> str:
    text_value = str(description).lower()

    if flow == "Revenue":
        if any(token in text_value for token in ("interest", "investment", "dividend", "asset sale")):
            return "Non-operating Revenue"
        return "Operating Revenue"

    if any(token in text_value for token in ("raw material", "inventory", "production", "manufacturing")):
        return "COGS"
    if any(token in text_value for token in ("rent", "salary", "insurance", "subscription")):
        return "Fixed OPEX"
    if any(token in text_value for token in ("marketing", "utility", "travel", "commission", "ad spend")):
        return "Variable OPEX"
    return "Variable OPEX"


def clean_financial_data(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str]:
    df = raw_df.copy()
    df.columns = [c.strip() for c in df.columns]

    amount_col = detect_amount_column(df)
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")

    date_cols = detect_date_columns(df.columns)
    if not date_cols:
        raise ValueError("No date-like column found. Expected column name with date/time.")
    date_col = date_cols[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Fill categorical columns with "Unknown"; numeric values with median.
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            median_value = df[col].median(skipna=True)
            if pd.isna(median_value):
                median_value = 0
            df[col] = df[col].fillna(median_value)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].ffill().bfill()
        else:
            df[col] = df[col].fillna("Unknown")

    df = df.dropna(subset=[date_col, amount_col]).copy()
    df["year"] = df[date_col].dt.year
    df["month"] = df[date_col].dt.month
    df["month_name"] = df[date_col].dt.strftime("%b")

    type_col = None
    for candidate in ("transaction_type", "type", "flow", "direction"):
        if candidate in map(str.lower, df.columns):
            actual = [c for c in df.columns if c.lower() == candidate]
            if actual:
                type_col = actual[0]
                break

    description_col = None
    for candidate in ("description", "details", "narration", "memo", "category"):
        if candidate in map(str.lower, df.columns):
            actual = [c for c in df.columns if c.lower() == candidate]
            if actual:
                description_col = actual[0]
                break
    if description_col is None:
        description_col = df.columns[0]

    df["flow_type"] = df.apply(
        lambda row: normalize_transaction_flow(row, amount_col=amount_col, type_col=type_col),
        axis=1,
    )
    df["abs_amount"] = df[amount_col].abs()
    df["financial_category"] = df.apply(
        lambda row: categorize_transaction(row[description_col], row["flow_type"]), axis=1
    )
    return df, date_col, amount_col


def write_dataframe_to_mysql(df: pd.DataFrame, engine: Engine, table_name: str) -> None:
    # Write in explicit chunks with progress logs so long loads are visible.
    total_rows = len(df)
    chunk_size = 10000
    if total_rows == 0:
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        log_step("No rows to insert; table recreated empty.")
        return

    total_chunks = (total_rows + chunk_size - 1) // chunk_size
    for chunk_idx, start in enumerate(range(0, total_rows, chunk_size), start=1):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]
        if_exists_mode = "replace" if chunk_idx == 1 else "append"
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists_mode,
            index=False,
            method="multi",
        )
        log_step(
            f"MySQL insert progress: chunk {chunk_idx}/{total_chunks} "
            f"({end:,}/{total_rows:,} rows)"
        )


def load_from_mysql(engine: Engine, table_name: str) -> pd.DataFrame:
    query = text(f"SELECT * FROM {table_name}")
    with engine.begin() as conn:
        return pd.read_sql(query, conn)


def calculate_financials(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summary = (
        df.groupby(["year", "month", "month_name"], as_index=False)
        .apply(
            lambda group: pd.Series(
                {
                    "cash_in": group.loc[group["flow_type"] == "Revenue", "abs_amount"].sum(),
                    "cash_out": group.loc[group["flow_type"] == "Expense", "abs_amount"].sum(),
                    "cogs": group.loc[group["financial_category"] == "COGS", "abs_amount"].sum(),
                    "total_expenses": group.loc[group["flow_type"] == "Expense", "abs_amount"].sum(),
                    "total_revenue": group.loc[group["flow_type"] == "Revenue", "abs_amount"].sum(),
                }
            )
        )
        .reset_index(drop=True)
    )

    summary["gross_profit"] = summary["total_revenue"] - summary["cogs"]
    summary["net_profit"] = summary["total_revenue"] - summary["total_expenses"]
    summary["monthly_cash_flow"] = summary["cash_in"] - summary["cash_out"]
    summary = summary.sort_values(["year", "month"]).reset_index(drop=True)

    expense_breakdown = (
        df[df["flow_type"] == "Expense"]
        .groupby("financial_category", as_index=False)["abs_amount"]
        .sum()
        .rename(columns={"abs_amount": "expense_amount"})
        .sort_values("expense_amount", ascending=False)
    )
    return summary, expense_breakdown


def create_visualizations(
    pnl_summary: pd.DataFrame, expense_breakdown: pd.DataFrame, output_dir: Path
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    labels = pnl_summary.apply(
        lambda row: f"{int(row['year'])}-{int(row['month']):02d}", axis=1
    )

    chart_paths = {
        "cash_flow_bar": output_dir / "monthly_cash_in_vs_out.png",
        "expense_pie": output_dir / "expense_category_breakdown.png",
        "net_profit_line": output_dir / "net_profit_trend.png",
    }

    plt.figure(figsize=(10, 6))
    x_pos = np.arange(len(pnl_summary))
    width = 0.4
    plt.bar(x_pos - width / 2, pnl_summary["cash_in"], width=width, label="Cash In")
    plt.bar(x_pos + width / 2, pnl_summary["cash_out"], width=width, label="Cash Out")
    plt.xticks(x_pos, labels, rotation=45)
    plt.ylabel("Amount")
    plt.title("Monthly Cash In vs Cash Out")
    plt.legend()
    plt.tight_layout()
    plt.savefig(chart_paths["cash_flow_bar"], dpi=150)
    plt.close()

    plt.figure(figsize=(8, 8))
    if expense_breakdown.empty:
        plt.text(0.5, 0.5, "No Expense Data", ha="center", va="center")
        plt.axis("off")
    else:
        plt.pie(
            expense_breakdown["expense_amount"],
            labels=expense_breakdown["financial_category"],
            autopct="%1.1f%%",
            startangle=120,
        )
        plt.title("Expense Category Breakdown")
    plt.tight_layout()
    plt.savefig(chart_paths["expense_pie"], dpi=150)
    plt.close()

    plt.figure(figsize=(10, 6))
    plt.plot(labels, pnl_summary["net_profit"], marker="o")
    plt.xticks(rotation=45)
    plt.ylabel("Net Profit")
    plt.title("Net Profit Trend")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(chart_paths["net_profit_line"], dpi=150)
    plt.close()

    return chart_paths


def generate_html_report(
    pnl_summary: pd.DataFrame, chart_paths: Dict[str, Path], output_dir: Path
) -> Path:
    report_path = output_dir / "financial_report.html"
    table_html = pnl_summary.to_html(index=False, border=0, classes="pnl-table")

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Financial Report Dashboard</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px;
      color: #222;
      background: #fafafa;
    }}
    h1, h2 {{
      margin-bottom: 10px;
    }}
    .card {{
      background: #fff;
      border: 1px solid #ddd;
      border-radius: 8px;
      padding: 16px;
      margin-bottom: 20px;
    }}
    .chart {{
      width: 100%;
      max-width: 1000px;
      height: auto;
      border: 1px solid #eee;
      border-radius: 6px;
      background: #fff;
      padding: 6px;
    }}
    .pnl-table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 14px;
    }}
    .pnl-table th, .pnl-table td {{
      border: 1px solid #ddd;
      padding: 8px;
      text-align: right;
    }}
    .pnl-table th {{
      background: #f1f3f5;
      text-align: center;
    }}
  </style>
</head>
<body>
  <h1>Financial Report Dashboard</h1>
  <div class="card">
    <h2>Profit & Loss Summary</h2>
    {table_html}
  </div>
  <div class="card">
    <h2>Monthly Cash In vs Cash Out</h2>
    <img class="chart" src="{chart_paths['cash_flow_bar'].name}" alt="Monthly Cash In vs Cash Out" />
  </div>
  <div class="card">
    <h2>Expense Category Breakdown</h2>
    <img class="chart" src="{chart_paths['expense_pie'].name}" alt="Expense Category Breakdown" />
  </div>
  <div class="card">
    <h2>Net Profit Trend</h2>
    <img class="chart" src="{chart_paths['net_profit_line'].name}" alt="Net Profit Trend" />
  </div>
</body>
</html>
"""
    report_path.write_text(html, encoding="utf-8")
    return report_path


def upload_to_google_sheets(
    df: pd.DataFrame, credentials_path: Path, sheet_name: str, worksheet_name: str
) -> None:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(credentials_path), scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open(sheet_name)
    worksheet = spreadsheet.worksheet(worksheet_name)

    worksheet.clear()  # idempotent refresh
    records = [df.columns.tolist()] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    worksheet.update("A1", records)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run dynamic financial ETL pipeline from file -> MySQL -> Google Sheets."
    )
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to Kaggle dataset (.csv, .xlsx, or .xls).",
    )
    parser.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", ""))
    parser.add_argument("--mysql-db", default=os.getenv("MYSQL_DB", "finance_db"))
    parser.add_argument("--mysql-table", default=os.getenv("MYSQL_TABLE", DEFAULT_TABLE))
    parser.add_argument(
        "--google-creds-path",
        default=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
        help="Path to Google service account JSON key",
    )
    parser.add_argument(
        "--google-sheet-name",
        default=os.getenv("GOOGLE_SHEET_NAME", ""),
        help="Google Sheet file name",
    )
    parser.add_argument(
        "--google-worksheet",
        default=os.getenv("GOOGLE_WORKSHEET_NAME", "Sheet1"),
        help="Worksheet tab name",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory where chart PNG files will be saved",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open generated HTML report in the default browser after run.",
    )
    return parser.parse_args()


def main() -> None:
    started = time.perf_counter()
    args = parse_args()

    csv_path = Path(args.csv_path).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not csv_path.exists():
        raise FileNotFoundError(f"Dataset file not found: {csv_path}")

    engine = build_mysql_engine(
        user=args.mysql_user,
        password=args.mysql_password,
        host=args.mysql_host,
        port=args.mysql_port,
        database=args.mysql_db,
    )

    log_step(f"Loading dataset from: {csv_path}")
    raw_df = load_input_dataset(csv_path)
    log_step(f"Loaded rows: {len(raw_df):,}")

    log_step("Cleaning and categorizing data...")
    cleaned_df, _, _ = clean_financial_data(raw_df)
    log_step(f"Cleaned rows: {len(cleaned_df):,}")

    log_step(f"Writing to MySQL table: {args.mysql_table}")
    write_dataframe_to_mysql(cleaned_df, engine, args.mysql_table)
    log_step("MySQL write complete.")

    log_step("Reading data back from MySQL...")
    db_df = load_from_mysql(engine, args.mysql_table)
    log_step(f"Rows read from MySQL: {len(db_df):,}")
    if "year" not in db_df.columns or "month" not in db_df.columns:
        db_df["year"] = pd.to_datetime(db_df.iloc[:, 0], errors="coerce").dt.year
        db_df["month"] = pd.to_datetime(db_df.iloc[:, 0], errors="coerce").dt.month
        db_df["month_name"] = pd.to_datetime(db_df.iloc[:, 0], errors="coerce").dt.strftime("%b")

    log_step("Calculating P&L metrics...")
    pnl_summary, expense_breakdown = calculate_financials(db_df)
    log_step("Generating chart PNG files...")
    chart_paths = create_visualizations(pnl_summary, expense_breakdown, output_dir)
    log_step("Generating browser HTML report...")
    html_report_path = generate_html_report(pnl_summary, chart_paths, output_dir)

    if args.google_creds_path and args.google_sheet_name:
        creds_path = Path(args.google_creds_path).expanduser().resolve()
        log_step("Uploading P&L summary to Google Sheets...")
        upload_to_google_sheets(
            pnl_summary, creds_path, args.google_sheet_name, args.google_worksheet
        )
        print(f"Google Sheet '{args.google_sheet_name}' updated successfully.")
    else:
        print("Google Sheets upload skipped (credentials path or sheet name not provided).")

    print("\nP&L Summary Preview:")
    print(pnl_summary.head(12).to_string(index=False))
    print("\nSaved chart files:")
    for key, path in chart_paths.items():
        print(f"- {key}: {path}")
    print(f"\nBrowser report: {html_report_path}")
    print("Open with: xdg-open \"{}\"".format(html_report_path))
    if args.open_browser:
        # Use the same mechanism that works reliably in this environment.
        subprocess.run(
            [sys.executable, "-m", "webbrowser", html_report_path.as_uri()],
            check=False,
        )
        print("Opened report in default browser.")
    elapsed = time.perf_counter() - started
    log_step(f"Pipeline finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
