"""
SiRiX Trader Data Pull (Single Trader)
-------------------------------------
Fetches a trader's full status block from the SiRiX REST endpoint
`/api/UserStatus/GetUserTransactions` and prints:

- Trader account info (balance, equity, margin, etc.)
- Open positions
- Pending orders   <-- added
- Closed positions (trade history)
- Monetary transactions (deposits/withdrawals/adjustments)
"""

import requests
from datetime import datetime
import json

# ======== CONFIGURATION ========
BASE_URL = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"
USER_ID = 117614  # Trader ID


def get_user_transactions(user_id, page=1, page_size=200):
    """Fetch user transactions from SiRiX API"""
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "UserId": user_id,
        "Page": page,
        "PageSize": page_size
    }

    response = requests.post(BASE_URL, headers=headers, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"[!] Failed to fetch data. Status Code: {response.status_code}")
        print(response.text)
        return None


def get_all_user_transactions(user_id, page_size=200, max_pages=50):
    """
    Pull multiple pages and merge ClosedPositions + MonetaryTransactions.
    Keeps UserData/OpenPositions/PendingOrders from page 1.
    """
    merged_closed = []
    merged_tx = []
    first_page_data = None

    for page in range(1, max_pages + 1):
        page_data = get_user_transactions(user_id, page=page, page_size=page_size)
        if page_data is None:
            break  # stop on error

        if first_page_data is None:
            first_page_data = page_data

        closed_p = page_data.get("ClosedPositions", []) or []
        tx_p = page_data.get("MonetaryTransactions", []) or []

        merged_closed.extend(closed_p)
        merged_tx.extend(tx_p)

        # Stop when both empty
        if not closed_p and not tx_p:
            break

    if first_page_data is None:
        return None

    first_page_data["ClosedPositions"] = merged_closed
    first_page_data["MonetaryTransactions"] = merged_tx
    return first_page_data


# ----- HELPERS -----
def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def sort_closed_positions(closed, newest_first=True):
    return sorted(closed, key=lambda t: _parse_iso(t.get("CloseTime")), reverse=newest_first)



def print_trader_info(data):
    """
    Print top-level trader identity + account balance info.
    Safely extracts nested dicts; missing fields print as None.
    """
    user = data.get("UserData", {})
    details = user.get("UserDetails", {})
    account = user.get("AccountBalance", {})

    print("\n[-] TRADER INFO:")
    print(f"User ID        : {user.get('UserID')}")
    print(f"Full Name      : {details.get('FullName')}")
    print(f"Email          : {details.get('Email')}")
    print(f"Phone          : {details.get('Phone')}")
    print(f"Country        : {details.get('Country')}")
    print(f"Currency       : {user.get('Currency')}")
    print(f"Balance        : {account.get('Balance')}")
    print(f"Equity         : {account.get('Equity')}")
    print(f"Free Margin    : {account.get('FreeMargin')}")
    print(f"Margin Level   : {account.get('MarginLevel')}")


def print_open_positions(data):
    """
    Print currently open positions (live trades).
    """
    print("\n[*] OPEN POSITIONS:")
    positions = data.get("OpenPositions", [])
    if not positions:
        print("None.")
        return
    for pos in positions:
        print(
            f"{pos.get('Symbol')} | Amount: {pos.get('Amount')} | "
            f"OpenRate: {pos.get('OpenRate')} | CurrentRate: {pos.get('CurrentRate')} | "
            f"P/L: {pos.get('Profit')}"
        )


def print_pending_orders(data):
    """
    Print pending orders that have not yet triggered.
    """
    print("\n[*] PENDING ORDERS:")
    orders = data.get("PendingOrders", [])
    if not orders:
        print("None.")
        return
    for o in orders:
        print(
            f"{o.get('CreationTime')} | {o.get('Symbol')} | "
            f"Type: {o.get('PendingOrderType')} | Amt: {o.get('Amount')} | "
            f"Price: {o.get('Price')} | SL: {o.get('StopLoss')} | TP: {o.get('TakeProfit')}"
        )

def print_closed_positions(data):
    """
    Print historical closed trades.
    """
    print("\n[*] CLOSED POSITIONS:")
    closed = data.get("ClosedPositions", [])
    if not closed:
        print("None.")
        return

    closed = sort_closed_positions(closed, newest_first=True)

    for trade in closed:
        print(
            f"{trade.get('CloseTime')} | {trade.get('Symbol')} | Amount: {trade.get('Amount')} | "
            f"Open: {trade.get('OpenRate')} | Close: {trade.get('CloseRate')} | "
            f"P/L: {trade.get('Profit')} | Total: {trade.get('TotalProfit')}"
        )
    print(f"\nTotal Closed Trades: {len(closed)}")


def print_monetary_transactions(data):
    """
    Print account cash activity (deposits, withdrawals, rebates, etc.).
    """
    print("\n[*] MONETARY TRANSACTIONS:")
    txs = data.get("MonetaryTransactions", [])
    if not txs:
        print("None.")
        return
    for t in txs:
        print(
            f"{t.get('Time')} | Type: {t.get('Type')} | Amount: {t.get('Amount')} | Comment: {t.get('Comment')}"
        )


if __name__ == "__main__":
    # Fetch data for the configured trader
    data = get_user_transactions(USER_ID)

    if not data:
        raise SystemExit(1)  # Stop if API failed

    # --- DEBUG ---
    print("\n[DEBUG] keys:", list(data.keys()))
    print("[DEBUG] counts:",
          "Closed", len(data.get("ClosedPositions", [])),
          "| Tx", len(data.get("MonetaryTransactions", [])))

    # Print sections in a logical order
    print_trader_info(data)
    print_open_positions(data)
    print_pending_orders(data)
    print_closed_positions(data)
    print_monetary_transactions(data)
