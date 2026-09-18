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
.PHONY: help up down install-hooks test test-python test-scala raw-uploader parquet-converter ingest clean

# .env está en formato KEY=value, que es sintaxis de Makefile válida — así no
# hace falta `source .env` (no funciona igual en Windows) y las variables se
# exportan a los subprocesos (sbt, docker compose) igual en Linux que en
# Windows, sin depender del shell.
-include .env
export

ifeq ($(OS),Windows_NT)
  PYTHON ?= python
  INSTALL_HOOKS_CMD = powershell -ExecutionPolicy Bypass -File scripts/install-githooks.ps1
else
  PYTHON ?= python3
  INSTALL_HOOKS_CMD = ./scripts/install-githooks.sh
endif

COMPOSE = docker compose -f docker/docker-compose.yml --env-file .env

help:
	@echo "Targets disponibles:"
	@echo "  up                 Levanta MinIO + Airflow + Postgres (docker compose)"
	@echo "  down               Para el stack de docker compose"
	@echo "  install-hooks      Instala los git hooks de .githooks/ (core.hooksPath)"
	@echo "  test               Corre los tests de Python y de Scala"
	@echo "  test-python        Corre solo los tests de Python (pytest)"
	@echo "  test-scala         Corre solo los tests de Scala (sbt test)"
	@echo "  raw-uploader       Lanza la app Spark L1 raw-uploader (primer plano)"
	@echo "  parquet-converter  Lanza la app Spark L2 parquet-converter (primer plano)"
	@echo "  ingest             Dispara manualmente el DAG de descarga en Airflow"
	@echo "  clean              Borra directorios de staging/checkpoints generados"

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

install-hooks:
	$(INSTALL_HOOKS_CMD)

test: test-python test-scala

test-python:
	$(PYTHON) -m pytest -q

test-scala:
	sbt -batch test

raw-uploader:
	sbt -batch "runMain raillytics.ingesta.RawUploaderApp"

parquet-converter:
	sbt -batch "runMain raillytics.ingesta.ParquetConverterApp"

ingest:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ingesta_data_sources

clean:
	$(PYTHON) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/bronze_l1_done', 'data/bronze_processed', 'data/checkpoints', 'target', 'project/target']]"
