import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import urllib


# === 1. Output folder path  ===
output_folder = Path("C:/Users/anish/OneDrive/Desktop/Anish/CRM API/CRM Dashboard")  # Directory
output_folder.mkdir(parents=True, exist_ok=True)

# === 2. Tables to export ===
tables = {
    "Lv_tpaccount": "dbo.Lv_tpaccount",
    "Account": "dbo.Account",
    "Lv_monetarytransaction": "dbo.Lv_monetarytransaction"
}

# === 3. Encode ODBC connection parameters ===
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=crmrepl3.LEVERATETECH.COM,1433;"
    "DATABASE=etwotprop_mscrm;"
    "UID=Repl_eTwotprop;"
    "PWD=vCm5ZzkkLQ9Pa6imLY2P;"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

# === 4. Create SQLAlchemy engine ===
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# === 5. Extract and export each table ===
for name, table in tables.items():
    try:
        print(f"[OK] Extracting {table}...")
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        output_file = output_folder / f"{name}.xlsx"
        df.to_excel(output_file, index=False)
        print(f"[OK]] Saved: {output_file}")
    except Exception as e:
        print(f"[ERROR] Failed to extract {table}: {e}")

print("[DONE] ~ All exports complete.")