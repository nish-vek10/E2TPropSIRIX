import requests

API_URL  = "https://restapi-real3.sirixtrader.com/api/UserStatus/GetUserTransactions"
TOKEN    = "t1_a7xeQOJPnfBzuCncH60yjLFu"
USER_ID  = 117614
PAGE     = 1
PAGE_SIZE = 200

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

payload = {
    "UserId": USER_ID,
    "Page": PAGE,
    "PageSize": PAGE_SIZE,
}

resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)

print("Status:", resp.status_code)
if resp.status_code != 200:
    print("Body:", resp.text)
    raise SystemExit(1)

data = resp.json()

print("Top-level keys:", list(data.keys()))
print("ClosedPositions:", len(data.get("ClosedPositions", [])))
print("OpenPositions:", len(data.get("OpenPositions", [])))
print("PendingOrders:", len(data.get("PendingOrders", [])))
print("MonetaryTransactions:", len(data.get("MonetaryTransactions", [])))
