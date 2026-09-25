# RAILLYTICS

**Raillytics Light — Plataforma de Ingeniería de Datos para Análisis y Predicción de Demanda Ferroviaria en España**

Proyecto de TFM (Máster en Ingeniería de Datos — Grupo 3). Plataforma end-to-end que integra, procesa y analiza datos ferroviarios públicos (Renfe Open Data, AEMET, festivos BOE, INE) para generar insights operativos y predicciones de demanda a 30 días.

**Stack tecnológico:** Python (ingesta) · Apache Airflow (orquestación) · MinIO — S3-compatible (almacenamiento Bronze/Silver/Gold) · Spark Structured Streaming en Scala (subida a Bronze L1/L2) · PySpark (procesamiento Silver) · Delta Lake + Parquet (almacenamiento) · DuckDB (modelo dimensional Gold en local; dbt + Snowflake en el diseño objetivo) · Apache Superset (dashboards en local; Power BI en el diseño objetivo) · Scikit-learn (modelo predictivo).

**Arquitectura:** patrón Medallion — Bronze (datos brutos) → Silver (datos limpios y enriquecidos) → Gold (modelo dimensional listo para consumo analítico).

---

## Equipo

- Elena Calcerrada Quiles
- Joaquín Ayllón Moreno
- Laura Rodríguez Mora
- Miguel Ángel Vicente Vicente
- Rubén Martínez Sierra

---

## Arquitectura de datos (Medallion)

Los datos fluyen por tres capas de calidad creciente:

```
Fuentes (Renfe, AEMET, BOE, INE)
        │  ingesta Python
        ▼
🥉 BRONZE ──► 🥈 SILVER ──► 🥇 GOLD ──────────────► Superset
   datos        PySpark        DuckDB construye        (DuckDB lee el Parquet
   en bruto     limpieza       el modelo dimensional    de Gold en MinIO)
   (MinIO)      (MinIO)        y lo deja en Parquet
                               (MinIO)         └─ diseño objetivo: dbt ► Snowflake ► Power BI
```

- **🥉 Bronze — datos en bruto**: un [framework de ingesta](#framework-de-ingesta-bronze) descarga las fuentes públicas y las promueve a MinIO en dos subcapas — `l1-raw` (tal cual llegan, sin transformar) y `l2` (mismo dato convertido a Parquet) — particionadas por fuente y fecha. Si algo falla después, siempre se puede volver al dato original en `l1-raw`.
- **🥈 Silver — datos limpios y enriquecidos**: jobs PySpark eliminan duplicados, tratan nulos, normalizan formatos (fechas, nombres de estaciones) y cruzan los viajeros con meteorología y festivos. Es la capa de "datos fiables".
- **🥇 Gold — datos listos para el análisis**: el modelo dimensional (dimensiones `Dim_Estacion`, `Dim_Linea`, `Dim_Fecha` y hechos `Fact_Viajeros`, `Fact_Puntualidad`). En este repositorio lo construye [DuckDB](#capa-gold-con-duckdb-y-dashboards-en-superset) con SQL a partir de Silver y lo deja como Parquet en el bucket `raillytics-gold`; Superset lo consulta directamente desde ahí (también con DuckDB) y el modelo predictivo de Scikit-learn puede leerlo igual. En el diseño del TFM este paso es dbt → Snowflake → Power BI: el SQL es el mismo y puede migrarse a modelos dbt cuando toque.

---

## Estructura de directorios

```
RAILLYTICS/
├── README.md
├── G3.pdf                      # Documento de diseño del proyecto
├── Makefile                    # Targets para levantar infra y lanzar ingesta (Linux/Windows)
├── requirements.txt            # Dependencias Python del proyecto
├── .env.example                # Plantilla de variables de entorno (API keys, credenciales, MinIO, Airflow)
│
├── config/
│   └── data_sources.yml        # Registro de fuentes (id, url, formato) — lo leen Python y Scala
│
├── docker/
│   ├── docker-compose.yml      # MinIO + Postgres + Airflow (LocalExecutor) + Superset, local/desarrollo
│   ├── init-buckets.sh         # Crea los buckets de MinIO (raillytics-bronze/-silver/-gold)
│   ├── postgres/               # init-databases.sh: crea la BD de Superset en el Postgres compartido con Airflow
│   └── superset/               # Imagen de Superset con driver DuckDB, superset_config.py y scripts de arranque/importación
│
├── dags/
│   ├── ingesta_data_sources.py # DAG Airflow: descarga por fuente (dynamic task mapping sobre el YAML)
│   └── gold_duckdb.py          # DAG Airflow: construye la capa Gold con DuckDB (disparo manual)
│
├── dashboards/
│   └── superset/raillytics_gold/  # Dashboards de Superset como código (Demanda, Puntualidad, Trazabilidad de cargas); se importan al arrancar
│
├── data/                       # Data Lake local — NO se versiona en git
│   ├── bronze/                 # Staging local por fuente — aquí escribe la descarga Python
│   ├── bronze_l1_done/         # Generado en runtime: ficheros ya subidos a MinIO L1, pendientes de L2
│   ├── bronze_processed/       # Generado en runtime: ficheros que ya completaron L1 y L2
│   ├── checkpoints/            # Generado en runtime: checkpoints de Spark Structured Streaming
│   ├── silver/                 # Datos limpios, normalizados y enriquecidos (Delta Lake)
│   └── gold/                   # Catálogo DuckDB local con vistas sobre Gold (make 04_gold), para notebooks
│
├── python/
│   └── raillytics/
│       ├── ingesta/            # sources.py (registro YAML), formats.py, filenames.py, download.py (descarga a staging)
│       ├── procesamiento/      # Jobs PySpark de limpieza y enriquecimiento (capa Silver); silver_sample.py: Silver sintético
│       ├── gold/               # build.py (modelo dimensional con DuckDB), sql/ (una consulta por tabla Gold)
│       ├── ml/                 # Modelo predictivo Scikit-learn (features, entrenamiento, evaluación)
│       └── utils/              # Comunes: fs.py (escritura atómica), lake.py (DuckDB + MinIO), cargas.py (trazabilidad de cargas)
│
├── build.sbt                   # Proyecto SBT (Scala 2.13 / Spark 4.2) en la raíz para que IntelliJ lo reconozca
├── project/                    # Metadatos del build SBT (build.properties)
├── src/
│   ├── main/scala/raillytics/
│   │   ├── common/             # Compartido entre jobs: logging, spark (SparkSessionFactory), fs, lake (BronzePaths)
│   │   └── ingesta/
│   │       ├── IngestaEnv      # Defaults de entorno comunes a L1 y L2
│   │       ├── config/         # DataSource + DataSourceConfig (lectura de config/data_sources.yml)
│   │       ├── formats/        # SourceFormat: formatos soportados y sus opciones de lectura
│   │       ├── l1/             # RawUploaderApp (main) · RawUploader (lógica) · RawUploaderSettings (entorno)
│   │       └── l2/             # ParquetConverterApp (main) · ParquetConverter (lógica) · ParquetConverterSettings (entorno)
│   └── test/scala/raillytics/  # Tests ScalaTest (mismo árbol de paquetes que main)
│
├── dbt/                        # Proyecto dbt: transformaciones Silver → Gold
│   ├── models/
│   │   ├── staging/
│   │   └── marts/              # Dimensiones (Dim_Estacion, Dim_Linea, Dim_Fecha) y hechos (Fact_Viajeros, Fact_Puntualidad)
│   └── tests/                  # Tests de calidad dbt (unicidad, integridad referencial, rangos)
│
├── notebooks/                  # Notebooks de exploración y análisis (EDA, validación de fuentes)
│
├── dashboards/                 # Ficheros Power BI (.pbix) y documentación de los 4 dashboards
│
├── tests/
│   ├── ingesta/                # Tests pytest del registro de fuentes, nombres de fichero y la descarga
│   ├── procesamiento/          # Tests pytest del Silver sintético (contrato de columnas, festivos, determinismo)
│   ├── gold/                   # Tests pytest del modelo Gold sobre un lake local (sin MinIO)
│   └── utils/                  # Tests pytest de utilidades comunes (fs, lake, trazabilidad de cargas)
│
└── docs/                       # Documentación técnica y memoria del TFM
```

---

## Framework de ingesta Bronze

Para dar de alta una fuente nueva basta con añadir una entrada a `config/data_sources.yml`
(`id`, `name`, `url`, `format` — `csv` o `json`). El resto del pipeline no necesita cambios:

```
config/data_sources.yml
        │
        ▼
DAG Airflow "ingesta_data_sources"  (una tarea de descarga por fuente)
        ▼
data/bronze/<source>/                  ← Python escribe aquí (staging local)
        │
        ▼  App Scala "raw-uploader" (Spark Structured Streaming)
        │   copia el fichero tal cual a MinIO: raillytics-bronze/l1-raw/<source>/<fecha>/
        ▼
data/bronze_l1_done/<source>/
        │
        ▼  App Scala "parquet-converter" (Spark Structured Streaming, 1 query por fuente)
        │   convierte a Parquet en MinIO: raillytics-bronze/l2/<source>/<fecha>/
        ▼
data/bronze_processed/<source>/
```

Los tres directorios de `data/` son **hermanos, no anidados** — es un detalle de diseño
deliberado: Spark recorre recursivamente cualquier subdirectorio alcanzable bajo una ruta
que ya haya hecho *match* con un glob, así que anidarlos reintroduciría una carrera entre
las dos apps (cada una movería ficheros que la otra todavía no ha procesado).

### Arranque rápido (local)

Con Docker y `make` instalados (`choco install make` / `scoop install make` en Windows):

```bash
make install-dev-env        # crea .venv (Python 3.9–3.12), instala requirements.txt,
                            # activa los git hooks y copia .env.example -> .env
                            # (rellena las credenciales: nunca valores por defecto)
make up                     # levanta MinIO + Postgres + Airflow + Superset
make 00_ingest              # dispara el DAG de descarga una vez
make 01_raw-uploader        # en una terminal aparte — app L1 (queda en primer plano)
make 02_parquet-converter   # en otra terminal aparte — app L2 (queda en primer plano)
make 03_silver-sample       # Silver sintético en MinIO (mientras no existan los jobs PySpark)
make 04_gold                # construye la capa Gold con DuckDB -> Parquet en raillytics-gold
                            # Superset: http://localhost:8088 (SUPERSET_ADMIN_USER / _PASSWORD del .env)
```

`make help` lista todos los targets disponibles (`up`/`down`, `test`, `test-python`,
`test-scala`, `clean`, etc.).

`install-dev-env` es idempotente: solo recrea el venv si no existe y solo reinstala
dependencias si cambia `requirements.txt`. Por defecto crea el venv con `py -3.12` en
Windows y `python3` en Linux/macOS (numpy 1.26 y pyarrow 16 no tienen wheels para
Python 3.13+); se puede cambiar con `make install-dev-env VENV_BASE_PYTHON=python3.11`.
Los targets que necesitan dependencias Python (`test-python`, `03_silver-sample`,
`04_gold`) usan directamente el intérprete de `.venv`, así que no hace falta
activarlo antes de llamar a `make`.

---

## Capa Gold con DuckDB y dashboards en Superset

En local la capa Gold no necesita un data warehouse: **DuckDB** construye el modelo
dimensional leyendo Silver de MinIO y lo deja como Parquet en el bucket `raillytics-gold`,
y **Superset** lo consulta directamente desde ahí (también con DuckDB, en memoria dentro
del contenedor). Es el mismo modelo que en el diseño del TFM carga dbt en Snowflake; solo
cambian el motor y el destino.

```
Silver (Parquet en MinIO)              Gold (Parquet en MinIO)                Superset (http://localhost:8088)
raillytics-silver/                     raillytics-gold/
  viajeros_enriquecidos/    DuckDB       dim_fecha/       dim_estacion/       DuckDB en memoria + httpfs:
  puntualidad_enriquecida/  ───────►     dim_linea/       fact_viajeros/  ◄── read_parquet('s3://raillytics-gold/...')
                            build.py     fact_puntualidad/
```

### Cómo se ejecuta

| Paso | Comando | Qué hace |
| --- | --- | --- |
| Silver de ejemplo | `make 03_silver-sample` | Genera un Silver sintético y determinista (365 días, semilla 42) en `raillytics-silver`. Sustituye a los jobs PySpark mientras no existan y produce exactamente las columnas que Gold espera (el contrato está en `python/raillytics/procesamiento/silver_sample.py`). Los datos no son reales. |
| Gold | `make 04_gold` | `python -m raillytics.gold.build`: ejecuta `python/raillytics/gold/sql/<tabla>.sql` y escribe cada tabla en `s3://raillytics-gold/<tabla>/<tabla>.parquet` (full refresh). Además deja `data/gold/raillytics_gold.duckdb`, un catálogo con vistas sobre Gold para notebooks. |
| Dashboards | `make up` (o `make 05_superset-import`) | El servicio `superset-init` importa `dashboards/superset/raillytics_gold/` en cada arranque; `05_superset-import` repite la importación sin reiniciar. |

Airflow incluye el DAG `gold_duckdb` (sin planificación: `airflow dags trigger gold_duckdb`),
que ejecuta lo mismo que `make 04_gold` dentro del contenedor. Los dos comandos leen la
configuración de MinIO de las variables `MINIO_*` del `.env`; con `SILVER_ROOT` y `GOLD_ROOT`
se puede apuntar a directorios locales (así corren los tests de `tests/gold/`, sin MinIO).

### Modelo Gold

| Tabla | Grano | Columnas principales |
| --- | --- | --- |
| `dim_fecha` | día | `fecha_id` (AAAAMMDD), `fecha`, `anio`, `trimestre`, `mes`, `nombre_mes`, `dia_semana` (1 = lunes), `nombre_dia`, `es_fin_de_semana`, `es_festivo`, `festivo_nombre`, `estacion_anio` |
| `dim_estacion` | estación | `estacion_id`, `nombre`, `provincia`, `comunidad`, `latitud`, `longitud` |
| `dim_linea` | línea | `linea_id`, `nombre`, `tipo_tren`, `origen`, `destino` |
| `fact_viajeros` | día × estación × línea | `fecha_id`, `fecha`, `estacion_id`, `linea_id`, `viajeros`, `temperatura_media`, `precipitacion_mm`, `condicion_meteo` |
| `fact_puntualidad` | servicio (tren) | `fecha_id`, `fecha`, `linea_id`, `estacion_id`, `servicio_id`, `hora_prevista`, `hora_real`, `hora`, `retraso_min`, `estado`, `cancelado`, `es_puntual` (retraso ≤ 5 min), meteo |

### Superset

- **Conexión**: la base de datos `Raillytics Gold (DuckDB)` es `duckdb:///:memory:` con la
  extensión `httpfs` precargada. Al arrancar, el contenedor guarda las credenciales de MinIO del
  `.env` como *secret* persistente de DuckDB (`docker/superset/superset-run.sh` ejecuta
  `python -m raillytics.utils.lake persist-secret`), así el YAML versionado no contiene
  credenciales. En SQL Lab se puede consultar Gold tal cual:
  `SELECT * FROM read_parquet('s3://raillytics-gold/dim_linea/*.parquet')`.
- **Datasets**: dos datasets virtuales que hacen el *star join* de cada tabla de hechos con sus
  dimensiones (`viajeros_diarios` y `puntualidad_servicios`), con las métricas guardadas
  (`total_viajeros`, `media_viajeros_dia`, `pct_puntuales`, `retraso_medio`, `retraso_p90`...).
- **Dashboards de ejemplo** (los dos primeros del diseño): *Demanda ferroviaria* (viajeros por
  estación, evolución por tipo de tren, día de la semana, festivos, meteorología, comunidades) y
  *Puntualidad* (% puntuales, retraso medio, evolución mensual y diaria, tabla por línea, franja
  horaria × tipo de tren, meteorología, estaciones críticas), con filtros nativos de fechas,
  tipo de tren, comunidad y línea. El tercero, *Trazabilidad de cargas*, se describe más abajo.
- **Metastore**: Superset guarda sus metadatos en la base de datos `superset` del mismo Postgres
  que usa Airflow (servicio `postgres`, variables `POSTGRES_USER`/`POSTGRES_PASSWORD` del `.env`);
  `docker/postgres/init-databases.sh` la crea al inicializar el volumen.
- **Dashboards como código**: `dashboards/superset/raillytics_gold/` está en el formato de
  exportación de Superset (`metadata.yaml` + `databases/`, `datasets/`, `charts/`, `dashboards/`).
  Para cambiar un dashboard: edítalo en la UI, expórtalo (Dashboards → Export), descomprime el
  ZIP sobre esa carpeta y haz commit. Al importar, los dashboards se sobrescriben, pero la base
  de datos, los datasets y los charts se identifican por `uuid` y se conservan si ya existen
  (para rehacerlos desde el YAML hay que borrarlos antes en la UI o cambiar su `uuid`).
- **Imagen**: `docker/superset/Dockerfile` añade a `apache/superset:6.1.0` los drivers `duckdb`,
  `duckdb-engine` y `psycopg2` (metastore en Postgres) y deja `httpfs` instalada para no depender
  de internet en runtime. Se construye sola la primera vez; para reconstruirla:
  `docker compose -f docker/docker-compose.yml --env-file .env build superset`.
- **Limitaciones**: el nombre del bucket (`raillytics-gold`) va escrito en el SQL de los datasets;
  si cambias `MINIO_BUCKET_GOLD` hay que actualizarlo también ahí. No hay Redis ni Celery (un solo
  worker de gunicorn), suficiente para desarrollo.

### Trazabilidad de cargas

Cada proceso de carga deja constancia de lo que ha hecho en una tabla Parquet del lake,
`s3://raillytics-gold/_trazabilidad/cargas/` (un fichero por ejecución, una fila por tabla
cargada): `run_id`, `proceso`, `capa`, `tabla`, `origen`, `destino`, `filas`, `bytes`,
`inicio`/`fin` (UTC), `duracion_s`, `estado` (`ok`/`error`), `error`, `parametros` (JSON),
`lanzado_por` (`make`, `airflow:<dag>`, `cli`), `ejecutor` y `usuario`. Ya lo hacen la descarga
Bronze del DAG `ingesta_data_sources`, el Silver sintético y la construcción de Gold; las apps
Scala pueden sumarse escribiendo Parquet con las mismas columnas en ese prefijo.

Para instrumentar un proceso nuevo basta con envolverlo (`python/raillytics/utils/cargas.py`):

```python
from raillytics.utils.cargas import registrar_carga

with registrar_carga("silver_viajeros", "silver", layout, con, parametros={...}) as ejecucion:
    with ejecucion.tabla("viajeros_enriquecidos", origen=..., destino=...) as carga:
        ...                 # la carga propiamente dicha
        carga.filas = n
```

El registro se escribe al terminar, también si la carga falla (el error queda en la fila y la
excepción se propaga); si el registro no se puede escribir, se avisa en el log pero la carga no
falla por eso. `make cargas` lista las últimas ejecuciones desde la terminal, y el dashboard
*Trazabilidad de cargas* de Superset muestra ejecuciones, errores, filas cargadas por día y
tabla, duración por proceso, la última carga de cada tabla (en rojo si hace más de 24 h) y el
historial completo. `TRAZABILIDAD_ROOT` cambia la ubicación del registro (por defecto, dentro
del bucket Gold).

### Uso desde notebooks

```python
from dotenv import find_dotenv, load_dotenv
from raillytics.utils.lake import S3Settings, connect

load_dotenv(find_dotenv(usecwd=True))  # MINIO_* del .env (lo busca hacia arriba desde el directorio actual)
con = connect(S3Settings.from_env(), database="../data/gold/raillytics_gold.duckdb", read_only=True)
con.sql("SELECT tipo_tren, sum(viajeros) FROM fact_viajeros JOIN dim_linea USING (linea_id) GROUP BY 1").show()
```

(Rutas relativas a `notebooks/`; el kernel necesita `python/` en el `PYTHONPATH`, por ejemplo con
`sys.path.insert(0, "../python")` o instalando el paquete en modo editable.)

---

## Git hooks

El repositorio incluye hooks versionados en `.githooks/` (no en `.git/hooks/`, que no se versiona):

- **pre-commit**: si hay ficheros `.scala`/`.sbt`/`project/**` en staging, ejecuta `sbt compile` y bloquea el commit si falla.
- **pre-push**: si el push incluye cambios en `.scala`/`.sbt`/`project/**`, ejecuta `sbt test` y bloquea el push si falla.

Ambos se omiten (sin ejecutar sbt) si no hay cambios relevantes en Scala/SBT, para no ralentizar commits de Python/dbt/Airflow.

Activación local (una sola vez por clon del repo): `make install-hooks`, o directamente:

```bash
# Linux / macOS
./scripts/install-githooks.sh

# Windows (PowerShell)
.\scripts\install-githooks.ps1
```

Las tres formas hacen lo mismo: `git config core.hooksPath .githooks` (y en Linux/macOS marcan los hooks como ejecutables).

---

## Criterios de nombrado de ramas

| Rama         | Propósito                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------- |
| `main`       | Rama estable. Solo recibe merges desde `develop` vía Pull Request. Nunca se hace commit directo.   |
| `develop`    | Rama de integración. Todo el trabajo diario se fusiona aquí mediante PRs.                          |
| `feat_*`     | Nueva funcionalidad. Ej.: `feat_ingesta_renfe`, `feat_modelo_demanda`.                             |
| `fix_*`      | Corrección de errores. Ej.: `fix_nulos_aemet`.                                                     |
| `docs_*`     | Cambios de documentación. Ej.: `docs_memoria_tfm`.                                                 |
| `refactor_*` | Mejoras de código sin cambio funcional. Ej.: `refactor_jobs_silver`.                               |

Reglas de nombrado:

- Todo en **minúsculas**, palabras separadas por guion bajo (`_`).
- Nombre descriptivo y corto, en castellano: `feat_dashboard_puntualidad`, no `feat_cosas`.
- Una rama = un objetivo. Si la tarea crece, se divide en varias ramas.
- Las ramas parten siempre de `develop` actualizado (`git pull` antes de crearla).

---

## Modo de trabajo con Pull Requests

1. **Crear la rama** desde `develop`:
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feat_nombre_descriptivo
   ```
2. **Desarrollar y commitear** con mensajes claros en castellano, en imperativo y con prefijo del tipo de cambio: `feat: añade ingesta de festivos BOE`, `fix: corrige duplicados en viajeros`.
3. **Publicar la rama y abrir la PR** contra `develop`:
   ```bash
   git push -u origin feat_nombre_descriptivo
   ```
   La PR debe incluir: descripción del cambio, motivación y cómo probarlo.
4. **Revisión obligatoria**: al menos **1 aprobación** de otro miembro del equipo antes de fusionar. El autor no aprueba su propia PR.
5. **Merge a `develop`**: preferiblemente con *squash merge* para mantener el historial limpio. La rama se elimina tras el merge.
6. **Merge a `main`**: solo desde `develop`, al cerrar un hito (fin de capa Bronze, Silver, Gold, modelo, dashboards…), mediante PR revisada por el equipo.

Normas generales:

- Nunca hacer `push` directo a `main` ni a `develop`.
- PRs pequeñas y frecuentes: más fáciles de revisar que una PR gigante.
- Resolver los conflictos en la rama de la feature (rebase o merge de `develop` hacia la rama), nunca en `develop`.

Para descargar o levantar el entorno:
   ```bash
   docker compose -f docker/docker-compose.yml --env-file .env pull
   ```