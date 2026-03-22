import os, json
from config import ORDER_OFFSET, CANCEL_THRESHOLD

ORDENES_PATH = "data/ordenes_activas.json"

def _cargar_ordenes():
    if os.path.exists(ORDENES_PATH):
        try:
            with open(ORDENES_PATH) as f:
                return json.load(f)
        except:
            return {}
    return {}

def _guardar_ordenes(ordenes):
    with open(ORDENES_PATH, "w") as f:
        json.dump(ordenes, f, indent=2)

ordenes_activas = _cargar_ordenes()

def colocar_ordenes(mercado: dict, capital_asignado: float):
    mid      = mercado["midpoint"]
    token_id = mercado["token_id"]
    question = mercado["question"]
    size     = round(capital_asignado / 2, 2)

    bid_price = round(mid - ORDER_OFFSET, 3)
    ask_price = round(mid + ORDER_OFFSET, 3)

    # DRY-RUN: simula sin ejecutar
    print(f"[DRY-RUN] BID {size} USDC @ {bid_price} | ASK {size} USDC @ {ask_price}")
    print(f"[DRY-RUN] Mercado: {question[:60]}")

    ordenes_activas[token_id] = {
        "bid_id":   "DRY-BID",
        "ask_id":   "DRY-ASK",
        "midpoint": mid,
        "question": question[:60]
    }
    _guardar_ordenes(ordenes_activas)

def revisar_y_repostear(token_id: str, nuevo_mid: float):
    orden = ordenes_activas.get(token_id)
    if not orden:
        return False

    delta = abs(nuevo_mid - orden["midpoint"])
    if delta > CANCEL_THRESHOLD:
        print(f"[DRY-RUN] Precio movido {delta:.4f} — cancela y reposta @ {nuevo_mid}")
        del ordenes_activas[token_id]
        _guardar_ordenes(ordenes_activas)
        return True

    return False

def cancelar_todas():
    """Limpia todas las órdenes activas — útil al reiniciar en live."""
    ordenes_activas.clear()
    _guardar_ordenes(ordenes_activas)
    print("[MAKER] Todas las órdenes canceladas")

