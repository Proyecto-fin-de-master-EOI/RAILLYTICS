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

# Base de metadatos de Superset (dashboards, usuarios, permisos): su propia
# base de datos en el Postgres compartido con Airflow (servicio `postgres`).
# No es la fuente de datos analítica: esa es DuckDB sobre el bucket Gold.
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{user}:{password}@postgres:5432/{db}".format(
    user=quote_plus(os.environ.get("POSTGRES_USER", "raillytics")),
    password=quote_plus(os.environ["POSTGRES_PASSWORD"]),
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

# Paleta propia del proyecto, para que las gráficas no usen los colores por defecto de Superset.
# Es una lista de esquemas categóricos: una clave documentada y sin efecto sobre el arranque (si el
# nombre no existiera, Superset cae al esquema por defecto en vez de fallar). El tema GLOBAL
# (THEME_DEFAULT) no se toca a propósito: una clave mal puesta ahí impide arrancar el contenedor, y
# el aspecto de los dashboards se resuelve con su CSS, que es por dashboard y reversible.
EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "raillytics",
        "label": "Raillytics",
        "description": "Violeta de marca y acompañantes; el primero es el que usan las series únicas.",
        "isDefault": True,
        "colors": [
            "#6C3FD1",  # violeta de marca
            "#2BB3A3",  # verde azulado
            "#F2A33C",  # ámbar
            "#E2574C",  # rojo suave
            "#4C7DF0",  # azul
            "#9B6BE0",  # violeta claro
            "#17A398",  # verde
            "#C9478F",  # magenta
            "#8A94A6",  # gris
            "#D9B310",  # mostaza
        ],
    },
]

# Tema de marca. Superset 6 usa tokens de Ant Design: tocando colorPrimary y los de superficie
# cambia el color de TODA la interfaz (botones, enlaces, pestañas, bordes), que es lo que el CSS por
# dashboard no puede hacer. Se parte del tema base y solo se pisan los tokens que interesan: así una
# versión futura que añada tokens nuevos los sigue trayendo.
try:
    from superset.config import THEME_DEFAULT as _TEMA_BASE

    THEME_DEFAULT = {
        **_TEMA_BASE,
        "token": {
            **_TEMA_BASE.get("token", {}),
            "colorPrimary": "#6C3FD1",
            "colorLink": "#6C3FD1",
            "colorInfo": "#6C3FD1",
            "colorSuccess": "#16A34A",
            "colorWarning": "#D97706",
            "colorError": "#E2574C",
            "colorBgLayout": "#F6F7FB",
            "colorBgContainer": "#FFFFFF",
            "colorBorder": "#E8EAF2",
            "colorBorderSecondary": "#EEF0F6",
            "colorTextBase": "#1F2437",
            "borderRadius": 12,
            "borderRadiusLG": 14,
            "fontFamily": "'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif",
            "fontSize": 13,
        },
    }
except Exception:  # pragma: no cover - si cambia la forma del tema, mejor arrancar sin marca
    pass
