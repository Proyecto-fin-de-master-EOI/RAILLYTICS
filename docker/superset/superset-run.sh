#!/usr/bin/env bash
# Arranque del servidor web de Superset (servicio superset de docker-compose).
#
# Antes de arrancar guarda las credenciales de MinIO (.env) como secret
# persistente de DuckDB en el HOME del contenedor. Las conexiones que abre
# Superset (duckdb:///:memory:, definidas en dashboards/superset/.../databases/)
# lo cargan automáticamente y así leen s3://raillytics-gold/... sin que haya
# ninguna credencial en el YAML versionado ni en el metastore de Superset.
set -euo pipefail

python -m raillytics.gold.lake persist-secret

exec /app/docker/entrypoints/run-server.sh
