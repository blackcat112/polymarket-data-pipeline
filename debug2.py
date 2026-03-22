import requests

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

r = requests.get(f"{CLOB}/rewards/markets/current", timeout=10)
data = r.json().get("data", [])

# Filtra solo los 27 buenos
candidatos = [m for m in data
              if float(m.get("total_daily_rate", 0)) > 5
              and float(m.get("rewards_min_size", 999)) <= 25]

print(f"Candidatos: {len(candidatos)}\n")

# Prueba el primero paso a paso
m = candidatos[0]
condition_id = m["condition_id"]
print(f"Probando condition_id: {condition_id}")

# Test 1: Gamma API
print("\n--- TEST GAMMA API ---")
try:
    gm = requests.get(f"{GAMMA}/markets",
                      params={"condition_id": condition_id}, timeout=5)
    print(f"Status: {gm.status_code}")
    print(f"Respuesta: {gm.text[:300]}")
except Exception as e:
    print(f"ERROR: {e}")

# Test 2: Probar con clob_token_ids directamente
print("\n--- TEST CLOB MARKETS ---")
try:
    cm = requests.get(f"{CLOB}/markets/{condition_id}", timeout=5)
    print(f"Status: {cm.status_code}")
    print(f"Respuesta: {cm.text[:300]}")
except Exception as e:
    print(f"ERROR: {e}")

# Test 3: Buscar token via CLOB markets endpoint general
print("\n--- TEST CLOB MARKETS GENERAL ---")
try:
    cm2 = requests.get(f"{CLOB}/markets",
                       params={"condition_id": condition_id}, timeout=5)
    print(f"Status: {cm2.status_code}")
    print(f"Respuesta: {cm2.text[:400]}")
except Exception as e:
    print(f"ERROR: {e}")

