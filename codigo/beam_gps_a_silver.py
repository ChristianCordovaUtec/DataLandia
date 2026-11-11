import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.transforms.window import FixedWindows, TimestampedValue

ENTRADA = "datos/bronze/eventos_gps.jsonl"
CONDADOS = {"CEN": "Central", "NOR": "Norte", "SUR": "Sur", "EST": "Este",
            "OES": "Oeste", "NES": "Noreste", "SOE": "Suroeste", "COS": "Costa"}


def es_valido(e):
    return (e.get("vehiculo_id") is not None
            and e.get("velocidad_kmh") is not None
            and 0 <= e["velocidad_kmh"] <= 120)


def separar(e, n_particiones):
    return 0 if es_valido(e) else 1      # 0 = válido, 1 = rechazado


def enriquecer(e):
    e = dict(e)
    e["condado"] = CONDADOS[e["zona_id"].split("-")[0]]
    return e


def con_tiempo(e):
    momento = datetime.fromisoformat(e["momento"]).replace(tzinfo=timezone.utc)
    return TimestampedValue(e, momento.timestamp())


class Resumir(beam.DoFn):
    def process(self, elemento, ventana=beam.DoFn.WindowParam):
        zona, eventos = elemento
        eventos = list(eventos)
        n = len(eventos)
        yield {
            "ventana_inicio": ventana.start.to_utc_datetime().isoformat(),
            "zona_id": zona,
            "eventos": n,
            "vehiculos_distintos": len({e["vehiculo_id"] for e in eventos}),
            "velocidad_media_kmh": round(sum(e["velocidad_kmh"] for e in eventos) / n, 1),
            "porcentaje_ocupados": round(100 * sum(1 for e in eventos if e["ocupado"]) / n, 1),
        }


with beam.Pipeline() as p:
    eventos = (p
               | "Leer" >> beam.io.ReadFromText(ENTRADA)
               | "Parsear" >> beam.Map(json.loads))

    validos, rechazados = eventos | "Separar" >> beam.Partition(separar, 2)
    limpios = validos | "Enriquecer" >> beam.Map(enriquecer)

    (limpios
     | "Limpios a texto" >> beam.Map(json.dumps)
     | "Escribir limpios" >> WriteToText(
         "datos/silver/eventos_gps_limpios", file_name_suffix=".jsonl", shard_name_template=""))

    (rechazados
     | "Rechazados a texto" >> beam.Map(json.dumps)
     | "Escribir rechazados" >> WriteToText(
         "datos/silver/eventos_gps_rechazados", file_name_suffix=".jsonl", shard_name_template=""))

    (limpios
     | "Marcar tiempo" >> beam.Map(con_tiempo)
     | "Ventanas de 1 minuto" >> beam.WindowInto(FixedWindows(60))
     | "Clave por zona" >> beam.Map(lambda e: (e["zona_id"], e))
     | "Agrupar" >> beam.GroupByKey()
     | "Resumir" >> beam.ParDo(Resumir())
     | "Resumen a texto" >> beam.Map(json.dumps)
     | "Escribir resumen" >> WriteToText(
         "datos/silver/eventos_gps_por_zona_minuto", file_name_suffix=".jsonl", shard_name_template=""))


def contar(ruta):
    with open(ruta) as f:
        return sum(1 for _ in f)

print("eventos leídos:", contar(ENTRADA))
print("eventos válidos (Silver):", contar("datos/silver/eventos_gps_limpios.jsonl"))
print("eventos rechazados:", contar("datos/silver/eventos_gps_rechazados.jsonl"))
print("filas de resumen por zona y minuto:", contar("datos/silver/eventos_gps_por_zona_minuto.jsonl"))
