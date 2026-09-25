# dags/gold_duckdb.py
from __future__ import annotations

from airflow.decorators import dag, task


@dag(
    dag_id="gold_duckdb",
    # Sin planificación: se dispara a mano (make 04_gold lo hace en local, o
    # `airflow dags trigger gold_duckdb`) hasta que exista el DAG de Silver
    # del que deba colgar. Hoy Silver lo genera raillytics.procesamiento.silver_sample.
    schedule=None,
    catchup=False,
    tags=["gold", "duckdb"],
)
def gold_duckdb():
    @task
    def build_gold_tables() -> dict[str, int]:
        # Imports dentro de la tarea: el dag-processor no necesita duckdb para
        # parsear el DAG (se instala vía _PIP_ADDITIONAL_REQUIREMENTS).
        from raillytics.gold.build import build_gold
        from raillytics.utils.lake import LakeLayout, S3Settings, connect

        # Dentro de compose MINIO_ENDPOINT apunta a minio:9000 (ver docker-compose.yml).
        layout = LakeLayout.from_env()
        con = connect(S3Settings.from_env() if layout.uses_s3 else None)
        return build_gold(con, layout)

    build_gold_tables()


gold_duckdb()
