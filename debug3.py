import requests

CLOB = "https://clob.polymarket.com"

r = requests.get(f"{CLOB}/rewards/markets/current", timeout=10)
data = r.json().get("data", [])

candidatos = [m for m in data
              if float(m.get("total_daily_rate", 0)) > 5
              and float(m.get("rewards_min_size", 999)) <= 25]

# Ver respuesta completa del CLOB para el primer candidato
condition_id = candidatos[0]["condition_id"]
cm = requests.get(f"{CLOB}/markets/{condition_id}", timeout=5).json()

print("RESPUESTA COMPLETA CLOB:")
print(cm)
print("\nCAMPOS DISPONIBLES:")
print(list(cm.keys()))

