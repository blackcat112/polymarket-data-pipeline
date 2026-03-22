import json, os
from config import CAPITAL_TOTAL, RESERVA_PCT, MAX_MERCADO_PCT

capital_operativo = CAPITAL_TOTAL * (1 - RESERVA_PCT)
CAPITAL_PATH = "data/capital_en_uso.json"

def _cargar():
    if os.path.exists(CAPITAL_PATH):
        try:
            with open(CAPITAL_PATH) as f:
                return json.load(f)
        except:
            return {}
    return {}

def _guardar(d):
    with open(CAPITAL_PATH, "w") as f:
        json.dump(d, f, indent=2)

def capital_disponible():
    capital_en_uso = _cargar()
    return capital_operativo - sum(capital_en_uso.values())

def puede_entrar(token_id: str):
    max_por_mercado = CAPITAL_TOTAL * MAX_MERCADO_PCT
    disponible = capital_disponible()
    if disponible < 5.0:
        return False, 0
    asignar = min(disponible, max_por_mercado)
    return True, asignar

def registrar_entrada(token_id: str, capital: float):
    d = _cargar()
    d[token_id] = capital
    _guardar(d)

def registrar_salida(token_id: str):
    d = _cargar()
    d.pop(token_id, None)
    _guardar(d)

