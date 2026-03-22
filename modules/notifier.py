import os, json, logging, time, requests
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv
import schedule

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID        = os.getenv("CHAT_ID")
LOG_PATH       = Path("data/daily_log.json")
CAPITAL_PATH   = Path("data/capital_en_uso.json")
ORDENES_PATH   = Path("data/ordenes_activas.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Envío Telegram ──────────────────────────────────────────────
def send_telegram(msg: str):
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=data, timeout=10).raise_for_status()
        logger.info("✅ Telegram enviado")
    except Exception as e:
        logger.error(f"❌ Telegram error: {e}")

# ── Notificaciones instantáneas (llamadas desde main.py) ────────
def notify_entrada(question: str, capital: float, mid: float, pool: float, num_makers: int):
    msg = (
        f"🟢 *ENTRADA*\n"
        f"📌 `{question[:55]}`\n"
        f"💵 Capital: `{capital:.2f} USDC`\n"
        f"📊 Mid: `{mid}` | Pool: `${pool:.1f}/día`\n"
        f"👥 Makers: `{num_makers}` | Score: `{pool/max(num_makers,1):.1f}`"
    )
    send_telegram(msg)

def notify_reposteo(question: str, mid_viejo: float, mid_nuevo: float, delta: float):
    msg = (
        f"🔄 *REPOSTEO*\n"
        f"📌 `{question[:55]}`\n"
        f"📉 Mid: `{mid_viejo}` → `{mid_nuevo}` (Δ`{delta:.4f}`)"
    )
    send_telegram(msg)

def notify_salida(question: str, capital_liberado: float):
    msg = (
        f"🔴 *SALIDA* — mercado cerrado/desaparecido\n"
        f"📌 `{question[:55]}`\n"
        f"💵 Capital liberado: `{capital_liberado:.2f} USDC`"
    )
    send_telegram(msg)

def notify_error(contexto: str, error: str):
    msg = (
        f"⚠️ *ERROR en el bot*\n"
        f"📍 `{contexto}`\n"
        f"❗ `{str(error)[:200]}`"
    )
    send_telegram(msg)

# ── Reporte diario a las 23:00 ──────────────────────────────────
def send_daily_report():
    hoy = str(date.today())

    try:
        with open(LOG_PATH) as f:
            data = json.load(f)
    except:
        data = []

    eventos_hoy = [e for e in data if e.get("fecha") == hoy]

    # Capital en uso ahora mismo
    try:
        with open(CAPITAL_PATH) as f:
            capital_en_uso = json.load(f)
        total_en_uso = sum(capital_en_uso.values())
    except:
        total_en_uso = 0.0

    # Órdenes activas
    try:
        with open(ORDENES_PATH) as f:
            ordenes = json.load(f)
        n_ordenes = len(ordenes)
    except:
        n_ordenes = 0

    entradas  = [e for e in eventos_hoy if e.get("tipo") == "entrada"]
    salidas   = [e for e in eventos_hoy if e.get("tipo") == "salida"]
    reposteos = [e for e in eventos_hoy if e.get("tipo") == "reposteo"]
    rewards   = [e for e in eventos_hoy if e.get("tipo") == "reward_sim"]
    total_reward = sum(e.get("reward", 0) for e in rewards)

    # Desglose por mercado
    by_market = {}
    for e in rewards:
        k = e.get("mercado", "?")[:40]
        by_market[k] = by_market.get(k, 0) + e.get("reward", 0)

    lineas_mercados = ""
    for m, r in sorted(by_market.items(), key=lambda x: x[1], reverse=True)[:8]:
        lineas_mercados += f"  • `{m}` → *{r:.4f} USDC*\n"

    msg = (
        f"📊 *REPORTE DIARIO — {hoy}*\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"💰 *Rewards estimados:* `{total_reward:.4f} USDC`\n"
        f"💼 *Capital en uso:* `{total_en_uso:.2f} USDC`\n"
        f"📂 *Órdenes activas:* `{n_ordenes}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Entradas: `{len(entradas)}` | "
        f"🔴 Salidas: `{len(salidas)}` | "
        f"🔄 Reposteos: `{len(reposteos)}`\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"*Por mercado:*\n{lineas_mercados}"
    )
    send_telegram(msg)

# ── Scheduler ───────────────────────────────────────────────────
def main():
    logger.info("🚀 Notifier iniciado")
    schedule.every().day.at("23:00").do(send_daily_report)
    send_telegram("🤖 *Bot LIVE arrancado* — escuchando mercados...")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()

