import json, time
from datetime import datetime
from pathlib import Path

LOG_PATH = Path("data/daily_log.json")

def _load_log():
    if LOG_PATH.exists():
        with open(LOG_PATH, "r") as f:
            try:
                return json.load(f)
            except:
                return []
    return []

def _save_log(data):
    with open(LOG_PATH, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def simular_rewards_hora():
    """Simula rewards para la última hora basándose en entradas activas."""
    data = _load_log()
    if not data:
        return

    hoy = datetime.utcnow().date().isoformat()

    # Entradas de hoy
    entradas = [e for e in data if e.get("fecha") == hoy and e.get("tipo") == "entrada"]

    # Evitar contar dos veces la misma entrada
    # Usamos mercado como key simple
    mercados = {}
    for e in entradas:
        mercados[e["mercado"]] = e

    nuevos = 0
    for mercado, entrada in mercados.items():
        # Parsear pool_diario desde el detalle: "capital=25.00 USDC | mid=0.785 | pool=$40.0/día"
        detalle = entrada.get("detalle", "")
        try:
            parte_pool = detalle.split("pool=$")[1]
            pool_str = parte_pool.split("/día")[0]
            pool_diario = float(pool_str)
        except Exception:
            pool_diario = 0.0

        # Estimación muy conservadora:
        # Supón que capturas aprox. pool_diario / 24 / 10 por hora
        # (dividimos por 10 para ajustar por competencia y spread real)
        if pool_diario <= 0:
            continue

        reward_hora = (pool_diario / 24.0) / 10.0

        if reward_hora <= 0:
            continue

        evento = {
            "fecha": hoy,
            "hora": time.strftime("%H:%M:%S"),
            "tipo": "reward_sim",
            "mercado": mercado,
            "detalle": f"simulado {reward_hora:.4f} USDC en la última hora (pool_día={pool_diario})",
            "reward": round(reward_hora, 6)
        }
        data.append(evento)
        nuevos += 1

    if nuevos > 0:
        _save_log(data)
    return nuevos

