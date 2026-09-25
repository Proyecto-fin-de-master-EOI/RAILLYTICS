"""Configuración de Superset para el docker-compose de Raillytics.

Se carga vía SUPERSET_CONFIG_PATH (ver docker-compose.yml). Solo cubre lo
necesario para el entorno local: clave secreta, metastore en Postgres y
límites algo más holgados para consultar Parquet en MinIO con DuckDB.

La conexión a los datos (DuckDB en memoria sobre s3://raillytics-gold) no va
aquí: la define dashboards/superset/raillytics_gold/databases/ y las
credenciales de MinIO las toma DuckDB de su secret persistente, que crea
superset-run.sh al arrancar el contenedor.
"""
import os
from urllib.parse import quote_plus

# Obligatoria: Superset se niega a arrancar con la clave por defecto.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Base de metadatos de Superset (dashboards, usuarios, permisos). No es la
# fuente de datos analítica: esa es DuckDB sobre el bucket Gold.
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{user}:{password}@superset-postgres:5432/{db}".format(
    user=quote_plus(os.environ.get("SUPERSET_POSTGRES_USER", "superset")),
    password=quote_plus(os.environ["SUPERSET_POSTGRES_PASSWORD"]),
    db=os.environ.get("SUPERSET_POSTGRES_DB", "superset"),
)

# Cada consulta lee Parquet de MinIO: algo más de margen que el minuto por defecto.
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120

# Filas máximas por consulta de gráfico y en SQL Lab.
ROW_LIMIT = 50_000
SQL_MAX_ROW = 100_000

# Sin Redis ni Celery: el estado de filtros y de explore va al metastore
# (comportamiento por defecto de Superset) y gunicorn corre con un único
# worker (SERVER_WORKER_AMOUNT en docker-compose.yml), suficiente en desarrollo.
