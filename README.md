# Dynamic Financial Report Pipeline (MySQL + Browser Dashboard)

## Project Overview

This project builds a dynamic, Linux-ready financial analysis pipeline that:

1. Loads retail transaction data from a source file (`.csv` or `.xlsx`)
2. Cleans and standardizes it with Pandas/NumPy
3. Stores processed transactions in MySQL
4. Calculates core finance metrics (Gross Profit, Net Profit, Monthly Cash Flow, P&L)
5. Generates charts as PNG files
6. Produces a browser-friendly HTML report

The report is refreshed each run, so outputs stay up to date with source data changes.

---

## What the Pipeline Produces

- MySQL table:
  - `finance_db.financial_transactions`
- Image outputs:
  - `outputs/monthly_cash_in_vs_out.png`
  - `outputs/expense_category_breakdown.png`
  - `outputs/net_profit_trend.png`
- Browser report:
  - `outputs/financial_report.html`

---

## Technology Stack

- Python 3
- Pandas
- NumPy
- Matplotlib
- SQLAlchemy
- mysql-connector-python
- OpenPyXL (for `.xlsx` input)
- gspread + oauth2client (optional Google Sheets sync)

---

## Financial Logic Implemented

- Transaction categorization:
  - Revenue:
    - Operating Revenue
    - Non-operating Revenue
  - Expenses:
    - COGS
    - Fixed OPEX
    - Variable OPEX
- Gross Profit:
  - `total_revenue - cogs`
- Net Profit:
  - `total_revenue - total_expenses`
- Monthly Cash Flow:
  - `cash_in - cash_out`
- P&L summary grouped by:
  - `year`, `month`, `month_name`

---

## Data Source

- Original dataset used:
  - `Online Retail.xlsx` (converted once to CSV for faster repeated runs)
- Recommended fast source path:
  - `/home/abel/Downloads/Online Retail.csv`

---

## Project Structure

- `run_pipeline.py`  
  End-to-end pipeline (file load -> clean -> MySQL sync -> calculations -> charts -> HTML report)

- `requirements.txt`  
  Dependencies

- `outputs/`  
  Generated charts and `financial_report.html`

---

## Environment Setup (Ubuntu)

```bash
cd "/home/abel/data analysis/financial report"
python3 -m venv ~/.venv
source ~/.venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

---

## MySQL Setup

Create DB and app user (if needed):

```bash
sudo mysql
```

```sql
CREATE DATABASE IF NOT EXISTS finance_db;

CREATE USER IF NOT EXISTS 'finance_user'@'localhost'
IDENTIFIED BY 'Fin@nc3_2026_Strong!';

GRANT ALL PRIVILEGES ON finance_db.* TO 'finance_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

---

## Run Commands

Activate environment:

```bash
cd "/home/abel/data analysis/financial report"
source ~/.venv/bin/activate
```

### Recommended (faster) run using CSV

```bash
python3 run_pipeline.py \
  --csv-path "/home/abel/Downloads/Online Retail.csv" \
  --mysql-host "127.0.0.1" \
  --mysql-port 3306 \
  --mysql-user "finance_user" \
  --mysql-password "Fin@nc3_2026_Strong!" \
  --mysql-db "finance_db" \
  --mysql-table "financial_transactions" \
  --output-dir "outputs" \
  --open-browser
```

### Run using XLSX (slower)

```bash
python3 run_pipeline.py \
  --csv-path "/home/abel/Downloads/Online Retail.xlsx" \
  --mysql-host "127.0.0.1" \
  --mysql-port 3306 \
  --mysql-user "finance_user" \
  --mysql-password "Fin@nc3_2026_Strong!" \
  --mysql-db "finance_db" \
  --mysql-table "financial_transactions" \
  --output-dir "outputs" \
  --open-browser
```

Open report manually (if needed):

```bash
python3 -m webbrowser "file:///home/abel/data%20analysis/financial%20report/outputs/financial_report.html"
```

---

## Optional: One-Time XLSX -> CSV Conversion

```bash
python3 -c "import pandas as pd; pd.read_excel('/home/abel/Downloads/Online Retail.xlsx').to_csv('/home/abel/Downloads/Online Retail.csv', index=False)"
```

---

## Verification Commands

Check table exists and has rows:

```bash
source ~/.venv/bin/activate
python3 - <<'PY'
import mysql.connector
conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="finance_user",
    password="Fin@nc3_2026_Strong!",
    database="finance_db",
)
cur = conn.cursor()
cur.execute("SHOW TABLES LIKE 'financial_transactions'")
print("table_exists:", bool(cur.fetchone()))
cur.execute("SELECT COUNT(*) FROM financial_transactions")
print("row_count:", cur.fetchone()[0])
cur.close()
conn.close()
PY
```

---

## Troubleshooting History

### 1) `externally-managed-environment` on pip install
- Cause: Ubuntu PEP 668 blocks global pip installs.
- Fix: Use virtual environment (`~/.venv`) and install there.

### 2) `ModuleNotFoundError` (e.g., `gspread`)
- Cause: Packages not installed in active interpreter.
- Fix: Activate venv and install from `requirements.txt`.

### 3) `python: command not found`
- Cause: System only had `python3`.
- Fix: Use `python3` explicitly.

### 4) Dataset path not found
- Cause: Placeholder paths used.
- Fix: Use real local path (`/home/abel/Downloads/Online Retail.xlsx` or `.csv`).

### 5) MySQL `Access denied for user 'root'@'localhost'`
- Cause: Ubuntu root/socket auth restrictions.
- Fix: Create dedicated DB user (`finance_user`) and use that in pipeline.

### 6) MySQL URI parse failure with special password chars
- Cause: Password containing `@` in raw URI string.
- Fix: Switched to `SQLAlchemy URL.create(...)` in code.

### 7) Perceived "stuck" behavior / no progress logs
- Cause: Long MySQL write and large file processing with minimal console output.
- Fix: Added stage logs + chunk-level MySQL progress logging.

### 8) Browser auto-open inconsistency
- Cause: `xdg-open` not reliable in this session.
- Fix: Use `python -m webbrowser file://...` path.

### 9) Missing Google service account key
- Cause: JSON key file not present locally.
- Fix: Keep cloud upload optional; local pipeline runs fully without cloud.

---

## Notes on Runtime

- Full run with large dataset may take several minutes.
- CSV input is significantly faster than XLSX.
- Current pipeline now reports progress so long steps are visible.

---

## Optional Google Sheets Sync (Currently Disabled by Choice)

The script supports Google Sheets upload if you pass:

- `--google-creds-path`
- `--google-sheet-name`
- `--google-worksheet`

If omitted, upload is skipped and local outputs still complete.

---

## Current Status

- Local pipeline: Working
- MySQL table population: Working
- Chart generation: Working
- HTML browser rendering: Working
- Google Sheets sync: Optional / not required for current workflow

# Financial-report
