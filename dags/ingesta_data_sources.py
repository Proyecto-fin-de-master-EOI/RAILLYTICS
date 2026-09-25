# dags/ingesta_data_sources.py
from __future__ import annotations

from pathlib import Path

from airflow.decorators import dag, task

from raillytics.ingesta.download import download
from raillytics.ingesta.sources import load_sources

# Rutas del lado del contenedor (ver docker-compose.yml: volúmenes
# ../config:/opt/airflow/raillytics_config y ../data:/opt/airflow/raillytics_data).
CONFIG_PATH = Path("/opt/airflow/raillytics_config/data_sources.yml")
BRONZE_STAGING_ROOT = Path("/opt/airflow/raillytics_data/bronze")


@dag(
    dag_id="ingesta_data_sources",
    schedule="@daily",
    start_date=None,
    catchup=False,
    tags=["ingesta", "bronze"],
)
def ingesta_data_sources():
    @task
    def download_source(source_id: str) -> str:
        # Import dentro de la tarea: el dag-processor no necesita duckdb para parsear el DAG.
        from raillytics.utils.cargas import registrar_carga

        sources_by_id = {s.id: s for s in load_sources(CONFIG_PATH)}
        source = sources_by_id[source_id]
        # Trazabilidad: una fila por descarga en <bucket gold>/_trazabilidad/cargas/
        # (MinIO y credenciales salen de las variables MINIO_* del contenedor).
        with registrar_carga("bronze_download", "bronze", parametros={"format": source.format}) as ejecucion:
            with ejecucion.tabla(source.id, origen=source.url) as carga:
                dest = download(source, BRONZE_STAGING_ROOT)
                carga.destino = str(dest)
                carga.bytes = dest.stat().st_size
        return str(dest)

    source_ids = [source.id for source in load_sources(CONFIG_PATH)]
    download_source.expand(source_id=source_ids)


ingesta_data_sources()
