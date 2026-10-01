# dags/ingesta_data_sources.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task

from raillytics.ingesta.download import download
from raillytics.ingesta.sources import load_sources

# Rutas del lado del contenedor (ver docker-compose.yml: volúmenes
# ../config:/opt/airflow/raillytics_config y ../data:/opt/airflow/raillytics_data).
CONFIG_PATH = Path("/opt/airflow/raillytics_config/data_sources.yml")
BRONZE_STAGING_ROOT = Path("/opt/airflow/raillytics_data/bronze")
# Cuarentena de los ficheros que no pasan los quality gates de descarga
# (raillytics.calidad.ficheros). Hermano del staging: L1 no lo ve.
BRONZE_REJECTED_ROOT = Path("/opt/airflow/raillytics_data/bronze_rejected")


@dag(
    dag_id="ingesta_data_sources",
    schedule="@daily",
    # Sin start_date Airflow 3 no programa el DAG aunque tenga schedule (solo
    # corre a mano). Con catchup=False no rellena el pasado: la primera
    # ejecución programada es la del día siguiente a despausarlo.
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingesta", "bronze"],
)
def ingesta_data_sources():
    @task
    def download_source(source_id: str) -> str:
        # Imports dentro de la tarea: el dag-processor no necesita duckdb para parsear el DAG.
        from raillytics.calidad import QualityGateError, registrar_calidad
        from raillytics.utils.cargas import registrar_carga

        sources_by_id = {s.id: s for s in load_sources(CONFIG_PATH)}
        source = sources_by_id[source_id]
        # Trazabilidad: una fila por descarga en <bucket gold>/_trazabilidad/cargas/ y una
        # por quality gate en .../calidad/, con el mismo run_id (MinIO y credenciales
        # salen de las variables MINIO_* del contenedor).
        with registrar_carga("bronze_download", "bronze", parametros={"format": source.format}) as ejecucion:
            with ejecucion.tabla(source.id, origen=source.url) as carga:
                descarga = download(source, BRONZE_STAGING_ROOT, BRONZE_REJECTED_ROOT)
                carga.destino = str(descarga.path)
                carga.bytes = descarga.bytes
                registrar_calidad(descarga.gates, "bronze_download", "bronze", ejecucion.run_id)
                # Fichero en cuarentena: la tarea falla (queda en rojo en Airflow y con
                # estado error en la trazabilidad) y el fichero no llega a L1.
                if not descarga.aceptada:
                    raise QualityGateError(descarga.gates)
        return str(descarga.path)

    source_ids = [source.id for source in load_sources(CONFIG_PATH)]
    download_source.expand(source_id=source_ids)


ingesta_data_sources()
