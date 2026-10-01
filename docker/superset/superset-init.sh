#!/usr/bin/env bash
# Inicialización de Superset (servicio superset-init de docker-compose). Se
# ejecuta en cada `make up` y es idempotente: migra el metastore, crea el
# usuario administrador si no existe, carga roles y permisos e importa los
# dashboards versionados en dashboards/superset/.
set -euo pipefail

: "${SUPERSET_ADMIN_USER:?SUPERSET_ADMIN_USER debe definirse en .env}"
: "${SUPERSET_ADMIN_PASSWORD:?SUPERSET_ADMIN_PASSWORD debe definirse en .env}"

echo ">> Migrando la base de metadatos de Superset"
superset db upgrade

echo ">> Usuario administrador '${SUPERSET_ADMIN_USER}'"
# create-admin no es idempotente: si el usuario ya existe imprime un error y
# no lo toca (la contraseña se cambia desde la UI). El `|| true` evita que un
# código de salida distinto de 0 en ese caso aborte el arranque.
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER}" \
    --firstname Admin \
    --lastname Raillytics \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@raillytics.local}" \
    --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo ">> Roles y permisos"
superset init

echo ">> Importando los dashboards de ejemplo"
bash "$(dirname "$0")/superset-import-dashboards.sh"

echo ">> Superset listo"
