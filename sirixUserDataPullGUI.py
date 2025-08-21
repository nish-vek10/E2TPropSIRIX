# Sirix Dashboard with GUI Output
# ----------------------------------
# This version fetches the data as before, but displays everything in a GUI dashboard
# with tabs for User Data, Open Positions, Pending Orders, Closed Positions, and Transactions.

import requests
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from datetime import datetime

# ----------- Config -----------
API_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"  # Replace in production

# ----------- Utility Formatting -----------
def _fmt_num(v, nd=2):
    return f"{v:,.{nd}f}" if isinstance(v, (int, float)) else v

def _fmt_dt(ts):
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return ts

# ----------- Fetch Data -----------
def fetch_data(user_id):
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "UserID": user_id,
        "GetOpenPositions": True,
        "GetPendingPositions": True,
        "GetClosePositions": True,
        "GetMonetaryTransactions": True
    }
    try:
        resp = requests.post(API_URL, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        messagebox.showerror("API Error", str(e))
        return None

# ----------- GUI Display -----------
def create_table(parent, data):
    if not data:
        ttk.Label(parent, text="No data available.").pack()
        return

    columns = list(data[0].keys())
    tree = ttk.Treeview(parent, columns=columns, show="headings")
    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, width=150)

    for row in data:
        values = [_fmt_dt(row.get(col)) if 'Time' in col else row.get(col) for col in columns]
        tree.insert('', 'end', values=values)

    tree.pack(expand=True, fill="both")


def show_dashboard(data):
    root = tk.Tk()
    root.title("SiRiX Dashboard")
    root.geometry("1200x700+200+100")

    tab_control = ttk.Notebook(root)

    # User Data
    user_tab = ttk.Frame(tab_control)
    user_info = data.get("UserData", {}).get("UserDetails", {})
    user_text = tk.Text(user_tab)
    for k, v in user_info.items():
        user_text.insert(tk.END, f"{k}: {_fmt_dt(v) if 'Time' in k else v}\n")
    user_text.pack(expand=True, fill="both")
    tab_control.add(user_tab, text="User Info")

    # Open Positions
    open_tab = ttk.Frame(tab_control)
    create_table(open_tab, data.get("OpenPositions", []))
    tab_control.add(open_tab, text="Open Positions")

    # Pending Orders
    pending_tab = ttk.Frame(tab_control)
    create_table(pending_tab, data.get("PendingOrders", []))
    tab_control.add(pending_tab, text="Pending Orders")

    # Closed Positions
    closed_tab = ttk.Frame(tab_control)
    create_table(closed_tab, data.get("ClosedPositions", []))
    tab_control.add(closed_tab, text="Closed Positions")

    # Monetary Transactions
    tx_tab = ttk.Frame(tab_control)
    create_table(tx_tab, data.get("MonetaryTransactions", []))
    tab_control.add(tx_tab, text="Transactions")

    tab_control.pack(expand=True, fill="both")
    root.mainloop()

# ----------- Entry Point -----------
def main():
    root = tk.Tk()
    root.withdraw()
    user_id = simpledialog.askstring("Enter User ID", "User ID:", parent=root)
    if not user_id:
        messagebox.showinfo("Cancelled", "No User ID provided.")
        return

    data = fetch_data(user_id.strip())
    if data:
        show_dashboard(data)

if __name__ == "__main__":
    main()
