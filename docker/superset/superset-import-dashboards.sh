#!/usr/bin/env bash
# Importa (o reimporta) en Superset los dashboards versionados en
# dashboards/superset/raillytics_gold/, que está en el formato de
# exportación de Superset (metadata.yaml + databases/ datasets/ charts/
# dashboards/). Superset solo acepta ese formato empaquetado en un ZIP con un
# directorio raíz, así que se empaqueta al vuelo.
#
# Lo usa superset-init.sh en cada arranque y `make 05_superset-import`
# (docker compose exec superset bash /app/raillytics/docker/superset-import-dashboards.sh).
#
# Semántica de la importación: los dashboards se sobrescriben; la base de
# datos, los datasets y los charts se identifican por uuid y, si ya existen,
# se conservan tal cual (para rehacerlos desde el YAML hay que borrarlos
# antes en la UI o cambiar su uuid).
set -euo pipefail

: "${SUPERSET_ADMIN_USER:?SUPERSET_ADMIN_USER debe definirse en .env}"

DASHBOARDS_DIR="${RAILLYTICS_DASHBOARDS_DIR:-/app/raillytics/dashboards}"
BUNDLE_NAME="raillytics_gold"
ZIP="$(mktemp -d)/${BUNDLE_NAME}.zip"

python - "$ZIP" "$DASHBOARDS_DIR" "$BUNDLE_NAME" <<'EOF'
import shutil, sys
zip_path, root_dir, base_dir = sys.argv[1:]
shutil.make_archive(zip_path[: -len(".zip")], "zip", root_dir, base_dir)
EOF

superset import-dashboards --path "$ZIP" --username "$SUPERSET_ADMIN_USER"
echo "Dashboards importados desde ${DASHBOARDS_DIR}/${BUNDLE_NAME}"
