#!/bin/sh
# Se ejecuta una sola vez, al inicializar el volumen de Postgres (la imagen
# oficial lanza lo que haya en /docker-entrypoint-initdb.d/). Crea la base de
# datos de Superset junto a la de Airflow (POSTGRES_DB), en el mismo servidor
# y con el mismo usuario: un único contenedor de Postgres para los metadatos
# de las dos herramientas.
#
# Si el volumen ya existía no vuelve a ejecutarse: crea la base a mano
#   docker compose -f docker/docker-compose.yml --env-file .env exec postgres createdb -U $POSTGRES_USER superset
# o recrea el volumen con `docker compose ... down -v` (borra los metadatos).
set -eu

if [ "${SUPERSET_POSTGRES_DB}" = "${POSTGRES_DB}" ]; then
    echo "SUPERSET_POSTGRES_DB coincide con POSTGRES_DB: Superset usará la misma base de datos"
    exit 0
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
    -c "CREATE DATABASE \"${SUPERSET_POSTGRES_DB}\" OWNER \"${POSTGRES_USER}\""
echo "Base de datos ${SUPERSET_POSTGRES_DB} creada para Superset"
