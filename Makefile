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
.PHONY: help up down install-dev-env install-hooks test test-python test-scala raw-uploader parquet-converter ingest clean

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

ifeq ($(OS),Windows_NT)
  # Spark/Hadoop en Windows necesita winutils.exe; el `export` de arriba lo
  # pasa a sbt y al resto de subprocesos. Sobreescribible desde el entorno o .env.
  HADOOP_HOME ?= C:\dev\winutils\hadoop-3.0.0
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

COMPOSE = docker compose -f docker/docker-compose.yml --env-file .env

help:
	@echo "Targets disponibles:"
	@echo "  up                 Levanta MinIO + Airflow + Postgres (docker compose)"
	@echo "  down               Para el stack de docker compose"
	@echo "  install-dev-env    Crea .venv, instala requirements.txt, git hooks y .env"
	@echo "  install-hooks      Instala los git hooks de .githooks/ (core.hooksPath)"
	@echo "  test               Corre los tests de Python y de Scala"
	@echo "  test-python        Corre solo los tests de Python (pytest)"
	@echo "  test-scala         Corre solo los tests de Scala (sbt test)"
	@echo "  00_ingest             Dispara manualmente el DAG de descarga en Airflow"
	@echo "  01_raw-uploader       Lanza la app Spark L1 raw-uploader (primer plano)"
	@echo "  02_parquet-converter  Lanza la app Spark L2 parquet-converter (primer plano)"
	@echo "  clean              Borra directorios de staging/checkpoints generados"

up:
	$(COMPOSE) up -d

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
	sbt -batch test

01_raw-uploader:
	sbt -batch "runMain raillytics.ingesta.l1.RawUploaderApp"

02_parquet-converter:
	sbt -batch "runMain raillytics.ingesta.l2.ParquetConverterApp"

00_ingest:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ingesta_data_sources

clean:
	$(PYTHON) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/bronze_l1_done', 'data/bronze_processed', 'data/checkpoints', 'target', 'project/target']]"
