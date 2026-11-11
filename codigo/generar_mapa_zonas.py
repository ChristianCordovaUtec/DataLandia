import csv

condados = ["CEN", "NOR", "SUR", "EST", "OES", "NES", "SOE", "COS"]
zonas = [f"{c}-{n:02d}" for c in condados for n in range(1, 7)]  # 48 zonas

filas = []
id_actual = 1
for i, zona in enumerate(zonas):
    cantidad = 6 if i < 23 else 5   # 23 grupos de 6 y 25 de 5 = 263
    for _ in range(cantidad):
        filas.append((id_actual, zona))
        id_actual += 1

filas.append((264, "SIN-ZONA"))
filas.append((265, "SIN-ZONA"))

with open("datos/referencia/mapa_zonas.csv", "w", newline="") as f:
    escritor = csv.writer(f)
    escritor.writerow(["location_id", "zona_id"])
    escritor.writerows(filas)

print("filas escritas:", len(filas))
