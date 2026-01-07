import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# ---------------- CONFIGURATION ----------------
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# --- Save location ---
SAVE_DIR = r"C:\Users\anish\OneDrive\Desktop\Anish\SIRIX DATA\Users"
os.makedirs(SAVE_DIR, exist_ok=True)

UTC_NOW = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = os.path.join(SAVE_DIR, f"all_users_{UTC_NOW}.csv")

# ---------------- COLUMN ORDER (EXACT) ----------------
ORDERED_FIELDS = [
    "UserID",
    "FullName",
    "Balance",
    "GroupName",
    "Leverage",
    "Country",
    "Phone",
    "Email",
    "Comment",
    "AgentAccountID",
    "RegistrationTime",
    "LastLoginTime",
    "Status",
    "City",
    "State",
    "Address",
    "ZipCode",
    "IDNumber",
    "DisablePasswordChange",
    "DisableTrading",
    "Tradability",
    "DisableSendReportsByEmail",
    "IsEnabled",
    "IsArchived",
    "SirixTT",
]


def fetch_all_users() -> list[dict]:
    url = f"{BASE_URL}/api/ManagementService/GetAllUsers"
    print("[-] Fetching users...")
    resp = requests.post(url, headers=HEADERS, json={})
    resp.raise_for_status()
    users = resp.json().get("Users", [])
    print(f"[DONE] {len(users)} users fetched.")
    return users


def save_users_csv(users: list[dict], output_file: str):
    if not users:
        raise ValueError("No users returned from API.")

    df = pd.DataFrame(users)

    # Ensure all expected columns exist (even if API omits some)
    for col in ORDERED_FIELDS:
        if col not in df.columns:
            df[col] = None

    # Any extra/unexpected columns → keep them, but move to the end
    extra_cols = [c for c in df.columns if c not in ORDERED_FIELDS]

    # Reorder
    df = df[ORDERED_FIELDS + extra_cols]

    print(f"[-] Writing CSV:\n    {output_file}")
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print("[DONE] CSV written successfully.")
    print(f"[INFO] Columns: {len(df.columns)} | Rows: {len(df)}")


def main():
    start = time.time()
    users = fetch_all_users()
    save_users_csv(users, OUTPUT_FILE)
    elapsed = int(time.time() - start)
    print(f"[DONE] Completed in {elapsed//60:02d}:{elapsed%60:02d} (MM:SS)")


if __name__ == "__main__":
    main()
