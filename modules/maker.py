import os, json, logging
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.constants import POLYGON
from config import ORDER_OFFSET, CANCEL_THRESHOLD

load_dotenv()

ORDENES_PATH = "data/ordenes_activas.json"

_client = None

def get_client():
    global _client
    if _client is None:
        proxy_url = os.getenv("PROXY_URL")
        _client = ClobClient(
            host="https://clob.polymarket.com",
            key=os.getenv("PRIVATE_KEY"),
            chain_id=POLYGON,
            funder=os.getenv("POLYMARKET_PROXY"),
            signature_type=2,
            proxy=proxy_url,
        )
        _client.set_api_creds(_client.create_or_derive_api_creds())
        logging.info(f"[MAKER] Cliente CLOB inicializado con proxy={proxy_url}")
    return _client

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

def colocar_ordenes(mercado: dict, capital_asignado: float) -> bool:
    mid      = mercado["midpoint"]
    token_id = mercado["token_id"]
    question = mercado["question"]
    size     = round(capital_asignado / 2, 2)

    bid_price = round(mid - ORDER_OFFSET, 3)
    ask_price = round(mid + ORDER_OFFSET, 3)

    try:
        client = get_client()

        bid_order = client.create_and_post_order(OrderArgs(
            token_id=token_id,
            price=bid_price,
            size=size,
            side="BUY",
        ))
        ask_order = client.create_and_post_order(OrderArgs(
            token_id=token_id,
            price=ask_price,
            size=size,
            side="SELL",
        ))

        bid_id = bid_order.get("orderID", "?")
        ask_id = ask_order.get("orderID", "?")

        logging.info(f"[LIVE] BID {size}@{bid_price} id={bid_id} | ASK {size}@{ask_price} id={ask_id}")

        ordenes_activas[token_id] = {
            "bid_id":   bid_id,
            "ask_id":   ask_id,
            "midpoint": mid,
            "question": question[:60]
        }
        _guardar_ordenes(ordenes_activas)
        return True

    except Exception as e:
        logging.error(f"[MAKER] Error colocando órdenes en {question[:40]}: {e}")
        return False

def _cancelar_ordenes_token(token_id: str):
    orden = ordenes_activas.get(token_id)
    if not orden:
        return
    try:
        client = get_client()
        for oid in [orden.get("bid_id"), orden.get("ask_id")]:
            if oid and oid not in ("?",):
                client.cancel(order_id=oid)
                logging.info(f"[MAKER] Orden cancelada: {oid}")
    except Exception as e:
        logging.warning(f"[MAKER] Error cancelando órdenes: {e}")

def revisar_y_repostear(token_id: str, nuevo_mid: float):
    orden = ordenes_activas.get(token_id)
    if not orden:
        return False

    delta = abs(nuevo_mid - orden["midpoint"])
    if delta > CANCEL_THRESHOLD:
        logging.info(f"[MAKER] Precio movido {delta:.4f} — cancela y reposta @ {nuevo_mid}")
        _cancelar_ordenes_token(token_id)
        del ordenes_activas[token_id]
        _guardar_ordenes(ordenes_activas)
        return True

    return False

def cancelar_todas():
    for token_id in list(ordenes_activas.keys()):
        _cancelar_ordenes_token(token_id)
    ordenes_activas.clear()
    _guardar_ordenes(ordenes_activas)
    logging.info("[MAKER] Todas las órdenes canceladas")

