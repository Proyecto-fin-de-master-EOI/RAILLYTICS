#!/bin/sh
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

mc mb --ignore-existing local/raillytics-bronze
mc mb --ignore-existing local/raillytics-silver
mc mb --ignore-existing local/raillytics-gold
