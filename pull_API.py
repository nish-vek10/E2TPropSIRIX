import requests

# --- Configuration ---
BASE_URL = "https://restapi-real3.sirixtrader.com"
TOKEN = "t1_a7xeQOJPnfBzuCncH60yjLFu"
TRADER_ID = "188320"  # Trader ID

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


def get_trader_full_status(trader_id):
    url = f"{BASE_URL}/api/UserStatus/GetUserTransactions"

    payload = {
        "UserId": trader_id,
        "Page": 1,
        "PageSize": 50  # adjust if you want more closed positions or transactions
    }

    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code == 200:
        data = response.json()

        # Print high-level summary
        user = data.get("UserData", {})
        account = user.get("AccountBalance", {})

        print("\n✅ Trader Info")
        print(f"Name: {user.get('UserDetails', {}).get('FullName')}")
        print(f"Balance: {account.get('Balance')}")
        print(f"Equity: {account.get('Equity')}")
        print(f"Free Margin: {account.get('FreeMargin')}")

        print("\n✅ Open Positions")
        for pos in data.get("OpenPositions", []):
            print(f"Symbol: {pos['Symbol']} | Amount: {pos['Amount']} | PnL: {pos['Profit']}")

        print("\n✅ Pending Orders")
        for order in data.get("PendingOrders", []):
            print(
                f"Symbol: {order['Symbol']} | Price: {order['Price']} | SL: {order['StopLoss']} | TP: {order['TakeProfit']}")

        print("\n✅ Closed Positions")
        for closed in data.get("ClosedPositions", []):
            print(f"Symbol: {closed['Symbol']} | Profit: {closed['Profit']} | Close Rate: {closed['CloseRate']}")

        print("\n✅ Monetary Transactions")
        for tx in data.get("MonetaryTransactions", []):
            print(f"Type: {tx['Type']} | Amount: {tx['Amount']} | Comment: {tx['Comment']}")

        return data
    else:
        print(f"❌ Error: {response.status_code} - {response.text}")
        return None

if __name__ == "__main__":
    get_trader_full_status(TRADER_ID)
