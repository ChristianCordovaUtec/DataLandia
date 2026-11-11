import json
import random
from datetime import datetime, timedelta

random.seed(42)

condados = ["CEN", "NOR", "SUR", "EST", "OES", "NES", "SOE", "COS"]
zonas = [f"{c}-{n:02d}" for c in condados for n in range(1, 7)]  # 48 zonas
vehiculos = [f"V{i:03d}" for i in range(1, 51)]                  # 50 vehículos
zona_actual = {v: random.choice(zonas) for v in vehiculos}

inicio = datetime(2024, 1, 15, 8, 0, 0)
n_eventos = 20000  # 20.000 eventos x 0,03 s = 10 minutos

with open("datos/bronze/eventos_gps.jsonl", "w") as f:
    for i in range(n_eventos):
        momento = inicio + timedelta(seconds=i * 0.03)
        v = random.choice(vehiculos)
        if random.random() < 0.1:                 # a veces el vehículo cambia de zona
            zona_actual[v] = random.choice(zonas)
        z = zona_actual[v]
        fila, col = divmod(zonas.index(z), 8)
        evento = {
            "vehiculo_id": v,
            "momento": momento.isoformat(),
            "zona_id": z,
            "latitud": round(40.70 + fila * 0.01 + random.uniform(0, 0.01), 5),
            "longitud": round(-74.02 + col * 0.01 + random.uniform(0, 0.01), 5),
            "velocidad_kmh": round(random.uniform(0, 60), 1),
            "ocupado": random.random() < 0.6,
        }
        r = random.random()
        if r < 0.01:
            evento["velocidad_kmh"] = round(random.uniform(150, 400), 1)  # error
        elif r < 0.02:
            evento["vehiculo_id"] = None                                   # error
        f.write(json.dumps(evento) + "\n")

print("eventos escritos:", n_eventos)
