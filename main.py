import time, json, logging, os
from datetime import date
from dotenv import load_dotenv
from modules.scanner import get_rewarded_markets
from modules.maker import colocar_ordenes, revisar_y_repostear, ordenes_activas, _guardar_ordenes, cancelar_todas
from modules.risk import puede_entrar, registrar_entrada, registrar_salida, _cargar, capital_disponible
from modules.notifier import notify_entrada, notify_reposteo, notify_salida, notify_error, send_daily_report
from config import SCAN_INTERVAL

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
    
    logging.info("🚀 Bot iniciado en modo LIVE")
    scan_count = 0
    reporte_enviado_hoy = False

    while True:
        try:
            scan_count += 1
            mercados = get_rewarded_markets()
            logging.info(f"Scanner #{scan_count}: {len(mercados)} oportunidades")

            # Liberar capital de mercados que ya no están en el scan
            token_ids_activos = {m["token_id"] for m in mercados}
            capital_en_uso = _cargar()

            for token_id in list(ordenes_activas.keys()):
                if token_id not in token_ids_activos:
                    nombre = ordenes_activas[token_id].get("question", token_id[:20])
                    capital_lib = capital_en_uso.get(token_id, 0.0)
                    registrar_salida(token_id)
                    del ordenes_activas[token_id]
                    _guardar_ordenes(ordenes_activas)
                    notify_salida(nombre, capital_lib)
                    log_evento(
                        tipo="salida",
                        mercado=nombre,
                        detalle=f"capital liberado={capital_lib:.2f} USDC"
                    )
                    logging.info(f"[SALIDA] {nombre} — capital liberado: {capital_lib:.2f} USDC")

            for m in mercados[:3]:
                token_id = m["token_id"]
                question = m["question"][:50]

                if token_id in ordenes_activas:
                    mid_viejo = ordenes_activas[token_id]["midpoint"]
                    repostear = revisar_y_repostear(token_id, m["midpoint"])
                    if repostear:
                        ok, capital = puede_entrar(token_id)
                        if ok:
                            colocar_ordenes(m, capital)
                            registrar_entrada(token_id, capital)
                            delta = abs(m["midpoint"] - mid_viejo)
                            notify_reposteo(question, mid_viejo, m["midpoint"], delta)
                            log_evento(
                                tipo="reposteo",
                                mercado=question,
                                detalle=f"mid {mid_viejo} → {m['midpoint']} (Δ{delta:.4f})"
                            )
                            logging.info(f"[REPOSTEO] {question} @ {m['midpoint']}")
                    else:
                        logging.info(f"[ACTIVO] {question} | mid={m['midpoint']} sin cambios")
                else:
                    ok, capital = puede_entrar(token_id)
                    if ok:
                        colocar_ordenes(m, capital)
                        registrar_entrada(token_id, capital)
                        notify_entrada(question, capital, m["midpoint"], m["pool_diario"], m["num_makers"])
                        log_evento(
                            tipo="entrada",
                            mercado=question,
                            detalle=f"capital={capital:.2f} USDC | mid={m['midpoint']} | pool=${m['pool_diario']}/día"
                        )
                        logging.info(f"[ENTRADA] {question} | capital={capital:.2f} | mid={m['midpoint']}")
                    else:
                        logging.info(f"[SKIP] Sin capital para: {question}")

            # Reporte diario a las 23:00 (una sola vez por día)
            hora_actual = time.strftime("%H:%M")
            if hora_actual == "23:00" and not reporte_enviado_hoy:
                send_daily_report()
                reporte_enviado_hoy = True
                logging.info("[REPORTE] Daily report enviado")
            elif hora_actual == "00:00":
                reporte_enviado_hoy = False  # reset para el día siguiente

        except Exception as e:
            logging.error(f"Error en loop: {e}")
            notify_error("loop principal", str(e))

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run()

