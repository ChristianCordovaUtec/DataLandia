#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

echo "== 1. Generar eventos GPS simulados (Bronze) =="
python codigo/generar_eventos_gps.py

echo "== 2. Streaming: Beam (Bronze a Silver) =="
python codigo/beam_gps_a_silver.py

echo "== 3. Lotes: Spark (Bronze a Silver) =="
python codigo/spark_bronze_a_silver_viajes.py

echo "== 4. dbt: construir Gold =="
cd datalandia_dbt
dbt run --profiles-dir .

echo "== 5. dbt: pruebas de calidad =="
dbt test --profiles-dir .

echo "== 6. Controles finales en Gold =="
python - << 'PY'
import duckdb
con = duckdb.connect("../datos/gold/datalandia.duckdb", read_only=True)
for t in ["dim_zona", "fact_viajes", "fact_flota_zona_minuto", "fact_demanda_vs_flota"]:
    print(t, con.sql(f"select count(*) from {t}").fetchone()[0])
PY
