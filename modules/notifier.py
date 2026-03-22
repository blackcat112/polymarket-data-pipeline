#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from modules.rewards_sim import simular_rewards_hora
from config import REPORT_HOUR
import asyncio
from pathlib import Path
import logging
from datetime import datetime, date
import json
import schedule
import time
from dotenv import load_dotenv
import requests

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger(__name__)

LOG_PATH = Path("data/daily_log.json")

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=data, timeout=10).raise_for_status()
        logger.info("✅ Telegram enviado")
    except Exception as e:
        logger.error(f"❌ Telegram error: {e}")

def send_daily_report():
    try:
        with open(LOG_PATH, "r") as f:
            data = json.load(f)
    except:
        logger.info("No daily_log encontrado")
        return

    hoy = str(date.today())
    eventos_hoy = [e for e in data if e.get("fecha") == hoy]
    
    if not eventos_hoy:
        logger.info("No eventos hoy")
        return

    # Estadísticas
    total_reward = sum(e.get("reward", 0) for e in eventos_hoy)
    entradas = len([e for e in eventos_hoy if e.get("tipo") == "entrada"])
    sims = len([e for e in eventos_hoy if e.get("tipo") == "reward_sim"])

    # Agrupar por mercado
    by_market = {}
    for e in eventos_hoy:
        if e.get("tipo") == "reward_sim":
            mercado = e.get("mercado", "Simulación")[:35]
            r = e.get("reward", 0)
            if mercado not in by_market:
                by_market[mercado] = 0
            by_market[mercado] += r

    msg = f"""🚀 *POLYMARKET BOT - Daily {hoy}* 🚀

💰 *TOTAL:* `{total_reward:.3f} USDC`

📈 *POSICIONES:* {entradas} | *SIMS:* {sims}

"""
    
    # Top mercados
    for mercado, reward in sorted(by_market.items(), key=lambda x: x[1], reverse=True)[:6]:
        msg += f"• `{mercado}` → *{reward:.3f}*\n"

    # Next events
    next_hour = (datetime.now().hour + 1) % 24
    msg += f"\n⏰ *Next:* {next_hour:02d}:00 hourly | 23:00 daily"

    send_telegram(msg)

async def hourly_rewards_job():
    try:
        reward = simular_rewards_hora()
        logger.info(f"💵 Reward: {reward:+.3f} USDC")

        evento = {
            "fecha": str(date.today()),
            "hora": datetime.now().strftime("%H:%M"),
            "tipo": "reward_sim",
            "mercado": "Daily pool sim",
            "detalle": f"simulado {reward:.4f} USDC",
            "reward": float(reward)
        }

        # Append to log
        try:
            with open(LOG_PATH, "r+") as f:
                try:
                    data = json.load(f)
                except:
                    data = []
                data.append(evento)
                f.seek(0)
                f.truncate()
                json.dump(data, f, indent=2)
        except FileNotFoundError:
            Path(LOG_PATH).parent.mkdir(exist_ok=True)
            with open(LOG_PATH, "w") as f:
                json.dump([evento], f, indent=2)

    except Exception as e:
        logger.error(f"Error hourly: {e}")

def main():
    logger.info("🚀 Notifier PRO iniciado")
    logger.info(f"Telegram: {'✅' if TELEGRAM_TOKEN and CHAT_ID else '❌ Config'}")

    # Schedule
    schedule.every().hour.at(":00").do(lambda: asyncio.run(hourly_rewards_job()))
    schedule.every().day.at(REPORT_HOUR).do(send_daily_report)
    
    # Trigger inicial
    asyncio.run(hourly_rewards_job())

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
