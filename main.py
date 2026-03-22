import time, json, logging, os
from datetime import date
from dotenv import load_dotenv
from modules.scanner import get_rewarded_markets
from modules.maker import colocar_ordenes, revisar_y_repostear, ordenes_activas
from modules.risk import puede_entrar, registrar_entrada
from config import SCAN_INTERVAL
from modules.risk import puede_entrar, registrar_entrada, registrar_salida
from modules.maker import colocar_ordenes, revisar_y_repostear, ordenes_activas, _guardar_ordenes

load_dotenv()
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def log_evento(tipo, mercado, detalle="", reward=0):
    path = "data/daily_log.json"
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except:
        data = []
    data.append({
        "fecha":   str(date.today()),
        "hora":    time.strftime("%H:%M:%S"),
        "tipo":    tipo,
        "mercado": mercado,
        "detalle": detalle,
        "reward":  reward
    })
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def run():
    logging.info("🚀 Bot iniciado en modo DRY-RUN")
    scan_count = 0

    while True:
        try:
            scan_count += 1
            mercados = get_rewarded_markets()
            logging.info(f"Scanner #{scan_count}: {len(mercados)} oportunidades")
        

        # Detectar mercados que ya no aparecen en el scan y liberar capital
        token_ids_activos = {m["token_id"] for m in mercados}
        for token_id in list(ordenes_activas.keys()):
            if token_id not in token_ids_activos:
                registrar_salida(token_id)
                del ordenes_activas[token_id]
                _guardar_ordenes(ordenes_activas)
                logging.info(f"[SALIDA] Mercado {token_id[:20]} ya no está en scan — capital liberado")
    
            for m in mercados[:3]:
                token_id = m["token_id"]
                question = m["question"][:50]

                if token_id in ordenes_activas:
                    # Ya estamos dentro — revisar si hay que repostear
                    repostear = revisar_y_repostear(token_id, m["midpoint"])
                    if repostear:
                        ok, capital = puede_entrar(token_id)
                        if ok:
                            colocar_ordenes(m, capital)
                            log_evento(
                                tipo="reposteo",
                                mercado=question,
                                detalle=f"nuevo mid={m['midpoint']}"
                            )
                            logging.info(f"[REPOSTEO] {question} @ {m['midpoint']}")
                    else:
                        logging.info(f"[ACTIVO] {question} | mid={m['midpoint']} sin cambios")
                else:
                    # Mercado nuevo — entrar
                    ok, capital = puede_entrar(token_id)
                    if ok:
                        colocar_ordenes(m, capital)
                        registrar_entrada(token_id, capital)
                        log_evento(
                            tipo="entrada",
                            mercado=question,
                            detalle=f"capital={capital:.2f} USDC | mid={m['midpoint']} | pool=${m['pool_diario']}/día"
                        )
                        logging.info(f"[ENTRADA] {question} | capital={capital:.2f} | mid={m['midpoint']}")
                    else:
                        logging.info(f"[SKIP] Sin capital para: {question}")

        except Exception as e:
            logging.error(f"Error en loop: {e}")

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run()

