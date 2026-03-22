import logging
import requests
from config import MIN_DAILY_REWARD, MAX_MAKERS, CAPITAL_TOTAL, MAX_MIN_SIZE, RESERVA_PCT

CLOB = "https://clob.polymarket.com"

def get_rewarded_markets():
    oportunidades = []

    try:
        r = requests.get(f"{CLOB}/rewards/markets/current", timeout=10)
        data = r.json().get("data", [])
        logging.info(f"[SCANNER] {len(data)} mercados totales")
    except Exception as e:
        logging.error(f"[SCANNER ERROR] {e}")
        return []

    capital_operativo = CAPITAL_TOTAL * (1 - RESERVA_PCT)

    for market in data:
        try:
            pool         = float(market.get("total_daily_rate", 0))
            min_size     = float(market.get("rewards_min_size", 999))
            condition_id = market.get("condition_id")

            if pool < MIN_DAILY_REWARD:
                continue
            if min_size > MAX_MIN_SIZE:
                logging.info(f"[SKIP min_size] {condition_id[:20]} min={min_size} pool={pool}")
                continue
            if min_size > capital_operativo:
                logging.info(f"[SKIP capital] {condition_id[:20]} min={min_size} cap={capital_operativo:.2f}")
                continue
    

            cm = requests.get(f"{CLOB}/markets/{condition_id}", timeout=5).json()

            if not cm.get("active") or cm.get("closed"):
                continue
            if not cm.get("accepting_orders"):
                continue

            question = cm.get("question", condition_id[:30])
            tokens = cm.get("tokens", [])
            if not tokens:
                continue

            token_id  = tokens[0].get("token_id")
            yes_price = float(tokens[0].get("price", 0.5))
            no_price  = float(tokens[1].get("price", 0.5)) if len(tokens) > 1 else 0.5
            max_spread = float(cm.get("rewards", {}).get("max_spread", 4.5))

            if not token_id:
                continue

            book = requests.get(
                f"{CLOB}/book",
                params={"token_id": token_id},
                timeout=5
            ).json()

            bids = book.get("bids", [])
            asks = book.get("asks", [])
            num_makers = len(bids) + len(asks)

            if num_makers > MAX_MAKERS:
                continue

            if bids and asks:
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                if best_bid > 0.05 and best_ask < 0.95:
                    midpoint = round((best_bid + best_ask) / 2, 4)
                else:
                    midpoint = round(yes_price, 4)
            elif yes_price > 0:
                midpoint = round(yes_price, 4)
            else:
                logging.info(f"[SKIP] Sin precio válido: {question[:40]}")
                continue

            score = pool / max(num_makers, 1)

            oportunidades.append({
                "condition_id": condition_id,
                "token_id":     token_id,
                "question":     question,
                "pool_diario":  pool,
                "min_size":     min_size,
                "max_spread":   max_spread,
                "num_makers":   num_makers,
                "score":        score,
                "midpoint":     midpoint,
                "yes_price":    yes_price,
                "no_price":     no_price
            })

            logging.info(f"[✅] {question[:55]} | pool=${pool:.1f} | makers={num_makers} | mid={midpoint}")

        except Exception as e:
            logging.warning(f"[SCANNER] Skipping: {e}")
            continue

    logging.info(f"[SCANNER] {len(oportunidades)} oportunidades válidas")
    return sorted(oportunidades, key=lambda x: x["score"], reverse=True)

