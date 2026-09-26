# Makefile — Raillytics: tareas de carga y procesamiento (capa Bronze)
#
# Requiere GNU Make. En Linux/macOS ya viene instalado.
# En Windows NO viene por defecto — instálalo con uno de:
#   choco install make
#   scoop install make
# (o usa WSL / Git Bash, que también lo suelen traer)
#
# Uso:  make            -> muestra esta ayuda
#       make <target>

.DEFAULT_GOAL := help
.PHONY: help up down install-dev-env install-hooks test test-python test-scala 00_ingest 01_raw-uploader 02_parquet-converter 03_silver-sample 04_gold 05_superset-import quality-gates cargas calidad clean

# .env está en formato KEY=value, que es sintaxis de Makefile válida — así no
# hace falta `source .env` (no funciona igual en Windows) y las variables se
# exportan a los subprocesos (sbt, docker compose) igual en Linux que en
# Windows, sin depender del shell.
-include .env
export

# Entorno virtual de desarrollo. VENV_BASE_PYTHON es el intérprete con el que
# se crea: debe ser 3.9–3.12 porque numpy 1.26 y pyarrow 16 no publican wheels
# para 3.13+. En Windows se usa el launcher `py` para elegir la versión aunque
# `python` apunte a otra. Se puede sobreescribir:
#   make install-dev-env VENV_BASE_PYTHON=python3.11
VENV := .venv

# Los módulos de python/raillytics se lanzan con `python -m` (targets 03/04);
# pytest ya resuelve python/ vía pyproject.toml, pero make tiene que exportarlo.
PYTHONPATH := python

ifeq ($(OS),Windows_NT)
  # Spark/Hadoop en Windows necesita winutils.exe: HADOOP_HOME se define en .env
  # (ver .env.example) y el `export` de arriba lo pasa a sbt y al resto de
  # subprocesos.
  PYTHON ?= python
  VENV_BASE_PYTHON ?= py -3.12
  VENV_PY = $(VENV)/Scripts/python
  INSTALL_HOOKS_CMD = powershell -ExecutionPolicy Bypass -File scripts/install-githooks.ps1
else
  PYTHON ?= python3
  VENV_BASE_PYTHON ?= python3
  VENV_PY = $(VENV)/bin/python
  INSTALL_HOOKS_CMD = ./scripts/install-githooks.sh
endif

# sbt siempre a través del wrapper: permite varios sbt a la vez en el proyecto
# (01_ y 02_ en paralelo, o 04_/quality-gates con ellos en marcha) sin el
# "Create a new server? y/n", y en Windows evita que Ctrl+C deje colgado el
# "¿Desea terminar el trabajo por lotes (S/N)?".
SBT = $(PYTHON) scripts/run_sbt.py

COMPOSE = docker compose -f docker/docker-compose.yml --env-file .env

help:
	@echo "Targets disponibles:"
	@echo "  up                 Levanta MinIO + Airflow + Postgres + Superset (docker compose)"
	@echo "  down               Para el stack de docker compose"
	@echo "  install-dev-env    Crea .venv, instala requirements.txt, git hooks y .env"
	@echo "  install-hooks      Instala los git hooks de .githooks/ (core.hooksPath)"
	@echo "  test               Corre los tests de Python y de Scala"
	@echo "  test-python        Corre solo los tests de Python (pytest)"
	@echo "  test-scala         Corre solo los tests de Scala (sbt test)"
	@echo "  00_ingest             Dispara manualmente el DAG de descarga en Airflow"
	@echo "  01_raw-uploader       Lanza la app Spark L1 raw-uploader (primer plano)"
	@echo "  02_parquet-converter  Lanza la app Spark L2 parquet-converter (primer plano)"
	@echo "  03_silver-sample      Genera un Silver sintético en MinIO (sustituto de los jobs PySpark)"
	@echo "  04_gold               Construye la capa Gold con la app Spark (Silver -> Parquet en raillytics-gold), con quality gates"
	@echo "  05_superset-import    Reimporta los dashboards de dashboards/superset/ en Superset"
	@echo "  quality-gates      Evalúa config/quality_gates.yml sobre Silver y Gold del lake (app Spark; falla si hay gates bloqueantes)"
	@echo "                     (make quality-gates QG_ARGS=silver | gold | <tabla> para acotar)"
	@echo "  cargas             Muestra las últimas cargas registradas (trazabilidad del lake)"
	@echo "  calidad            Muestra los últimos resultados de quality gates registrados"
	@echo "  clean              Borra directorios de staging/checkpoints generados"

up:
	$(COMPOSE) up -d

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down

# Todo en Python/make (sin if/test del shell) para que funcione igual con
# cmd.exe en Windows que con sh en Linux. Es idempotente: el venv y las
# dependencias solo se rehacen si falta el venv o cambia requirements.txt.
install-dev-env: $(VENV)/.deps-installed install-hooks
	$(VENV_PY) -c "import pathlib, shutil; p = pathlib.Path('.env'); print('OK .env ya existe') if p.exists() else (shutil.copyfile('.env.example', p), print('OK .env creado desde .env.example: rellena las credenciales'))"
	$(VENV_PY) -c "import shutil; m = [t for t in ('java', 'sbt', 'docker') if not shutil.which(t)]; print('AVISO: no estan en el PATH (necesarios para Scala/Docker): ' + ', '.join(m) if m else 'OK java, sbt y docker disponibles')"
	@echo "Entorno listo. Activa el venv con:"
	@echo "  Linux/macOS: source $(VENV)/bin/activate"
	@echo "  Windows:     $(VENV)\Scripts\Activate.ps1"

$(VENV)/pyvenv.cfg:
	$(VENV_BASE_PYTHON) -c "import sys; sys.exit(0 if (3, 9) <= sys.version_info[:2] <= (3, 12) else 'Se necesita Python 3.9-3.12 para el venv (encontrado %d.%d). Usa VENV_BASE_PYTHON=...' % sys.version_info[:2])"
	$(VENV_BASE_PYTHON) -m venv $(VENV)

$(VENV)/.deps-installed: $(VENV)/pyvenv.cfg requirements.txt
	$(VENV_PY) -m pip install --upgrade pip
	$(VENV_PY) -m pip install -r requirements.txt
	$(VENV_PY) -c "import pathlib; pathlib.Path('$@').touch()"

install-hooks:
	$(INSTALL_HOOKS_CMD)

test: test-python test-scala

# Usa directamente el python del venv: equivale a tenerlo activado, sin
# depender de `activate` (que es distinto en cada shell/SO).
test-python: $(VENV)/.deps-installed
	$(VENV_PY) -m pytest -q

test-scala:
	$(SBT) -batch test

00_ingest:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ingesta_data_sources

01_raw-uploader:
	$(SBT) -batch "runMain raillytics.ingesta.l1.RawUploaderApp"

02_parquet-converter:
	$(SBT) -batch "runMain raillytics.ingesta.l2.ParquetConverterApp"

# Silver sintético: corre en el host con el python del venv (como test-python)
# y habla con MinIO con las variables MINIO_* del .env.
03_silver-sample: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.procesamiento.silver_sample

# Gold: app Spark batch en Scala (misma configuración s3a que L1/L2). Aplica los
# quality gates de config/quality_gates.yml a Silver (entrada) y a Gold (salida,
# antes de escribir): si falla uno bloqueante, termina con error y no toca Gold.
04_gold:
	$(SBT) -batch "runMain raillytics.gold.GoldBuilderApp"

05_superset-import:
	$(COMPOSE) exec superset bash /app/raillytics/docker/superset-import-dashboards.sh

# Quality gate independiente sobre el lake: no escribe datos, solo evalúa y
# registra. Termina con error si falla algún gate bloqueante, así sirve de
# barrera entre pasos (p. ej. tras 03_silver-sample y antes de 04_gold).
QG_ARGS ?=
quality-gates:
	$(SBT) -batch "runMain raillytics.calidad.QualityGatesApp $(QG_ARGS)"

cargas: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.utils.cargas

calidad: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.calidad

clean:
	$(PYTHON) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/bronze_l1_done', 'data/bronze_processed', 'data/bronze_rejected', 'data/checkpoints', 'target', 'project/target']]"
