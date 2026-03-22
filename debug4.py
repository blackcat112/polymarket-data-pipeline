import requests

CLOB = "https://clob.polymarket.com"

# Coger los primeros 3 mercados válidos y ver su book completo
r = requests.get(f"{CLOB}/rewards/markets/current", timeout=10)
data = r.json().get("data", [])

candidatos = [m for m in data
              if float(m.get("total_daily_rate", 0)) > 5
              and float(m.get("rewards_min_size", 999)) <= 25][:3]

for m in candidatos:
    cid = m["condition_id"]
    cm = requests.get(f"{CLOB}/markets/{cid}", timeout=5).json()
    tokens = cm.get("tokens", [])
    if not tokens:
        continue

    token_id  = tokens[0].get("token_id")
    yes_price = tokens[0].get("price")
    no_price  = tokens[1].get("price") if len(tokens) > 1 else None

    book = requests.get(f"{CLOB}/book", params={"token_id": token_id}, timeout=5).json()

    print(f"\n{'='*50}")
    print(f"Mercado: {cm.get('question', '')[:60]}")
    print(f"YES price: {yes_price} | NO price: {no_price}")
    print(f"Bids: {book.get('bids', [])[:3]}")
    print(f"Asks: {book.get('asks', [])[:3]}")
    print(f"Spread máximo permitido: {cm.get('rewards',{}).get('max_spread')}")

