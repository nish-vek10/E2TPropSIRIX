import os
import pyodbc
import pandas as pd

# === 1. Connection details ===
server = "crmrepl3.LEVERATETECH.COM,1433"
database = "etwotprop_mscrm"
username = "Repl_eTwotprop"
password = "vCm5ZzkkLQ9Pa6imLY2P"  # <-- replace with secure handling in production

# === 2. Table list ===
tables = [
    "dbo.Lv_tpaccount",
    "dbo.Account",
    "dbo.Lv_monetarytransaction"
]

# === 3. Output Excel file ===
output_file = "crm_extract.xlsx"

# === 4. Connect to SQL Server ===
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};DATABASE={database};"
    f"UID={username};PWD={password};"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

try:
    conn = pyodbc.connect(conn_str)
except Exception as e:
    print("[ERROR] Failed to connect to the database.")
    print(str(e))
    exit(1)

# === 5. Extract and write each table to Excel worksheet ===
with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    for table in tables:
        print(f"[OK] Extracting {table}...")
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            sheet_name = table.split(".")[-1].strip("[]")[:31]  # Excel limit
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            print(f"[ERROR] Failed to export {table}: {e}")

conn.close()
print(f"[OK] Export complete: {output_file}")
