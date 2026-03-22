import requests

CLOB = "https://clob.polymarket.com"

r = requests.get(f"{CLOB}/rewards/markets/current", timeout=10)
data = r.json().get("data", [])

min_sizes = [float(m.get("rewards_min_size", 999)) for m in data]
rates = [float(m.get("total_daily_rate", 0)) for m in data]

print(f"Total mercados: {len(data)}\n")

print("=== DISTRIBUCIÓN rewards_min_size ===")
print(f"Mínimo: {min(min_sizes)}")
print(f"Máximo: {max(min_sizes)}")
print(f"Con min_size <= 10:  {sum(1 for x in min_sizes if x <= 10)}")
print(f"Con min_size <= 25:  {sum(1 for x in min_sizes if x <= 25)}")
print(f"Con min_size <= 50:  {sum(1 for x in min_sizes if x <= 50)}")
print(f"Con min_size <= 100: {sum(1 for x in min_sizes if x <= 100)}")

print("\n=== MERCADOS CON min_size <= 25 Y pool > 5 ===")
buenos = [m for m in data
          if float(m.get("rewards_min_size", 999)) <= 25
          and float(m.get("total_daily_rate", 0)) > 5]
print(f"Encontrados: {len(buenos)}")
for m in buenos[:5]:
    print(f"  pool={m['total_daily_rate']} | min_size={m['rewards_min_size']} | spread={m['rewards_max_spread']}")

print("\n=== MERCADOS CON pool > 20 (los mejores) ===")
top = sorted([m for m in data if float(m.get("total_daily_rate",0)) > 20],
             key=lambda x: float(x["total_daily_rate"]), reverse=True)
for m in top[:8]:
    print(f"  pool={m['total_daily_rate']:.1f}/día | min_size={m['rewards_min_size']} | spread={m['rewards_max_spread']}")

