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
        sources_by_id = {s.id: s for s in load_sources(CONFIG_PATH)}
        source = sources_by_id[source_id]
        dest = download(source, BRONZE_STAGING_ROOT)
        return str(dest)

    source_ids = [source.id for source in load_sources(CONFIG_PATH)]
    download_source.expand(source_id=source_ids)


ingesta_data_sources()
