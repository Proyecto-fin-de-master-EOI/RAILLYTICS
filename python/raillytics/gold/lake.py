"""Acceso con DuckDB al Data Lake (MinIO) para la capa Gold.

En local DuckDB hace el papel que en el diseño ocupa Snowflake: es el motor
SQL que construye el modelo dimensional a partir de Silver (build.py) y el
que usa Superset para consultar Gold. No hay servidor: cada proceso abre su
propia conexión (normalmente en memoria) y lee/escribe Parquet directamente
en los buckets de MinIO a través de la extensión httpfs.

La configuración sale de las mismas variables de entorno que ya usan las apps
Spark de ingesta (MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD,
MINIO_BUCKET_SILVER, MINIO_BUCKET_GOLD). SILVER_ROOT / GOLD_ROOT permiten
apuntar a directorios locales (tests, desarrollo sin MinIO).

Uso como script:  python -m raillytics.gold.lake persist-secret
  Guarda las credenciales de MinIO como secret persistente de DuckDB
  (~/.duckdb/stored_secrets), para procesos que abren sus propias conexiones
  sin pasar por este módulo: es lo que hace el contenedor de Superset al
  arrancar (docker/superset/superset-run.sh).
"""
from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import duckdb

logger = logging.getLogger(__name__)

# Nombre del secret de DuckDB con las credenciales de MinIO (temporal o persistente).
S3_SECRET_NAME = "raillytics_minio"


@dataclass(frozen=True)
class S3Settings:
    """Cómo llega DuckDB a MinIO. endpoint va sin esquema (host:puerto), como espera httpfs."""

    endpoint: str
    access_key: str
    secret_key: str
    use_ssl: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> S3Settings:
        access_key = env.get("MINIO_ROOT_USER", "")
        secret_key = env.get("MINIO_ROOT_PASSWORD", "")
        if not access_key or not secret_key:
            raise ValueError("MINIO_ROOT_USER y MINIO_ROOT_PASSWORD deben definirse (ver .env.example)")
        raw = env.get("MINIO_ENDPOINT", "http://localhost:9000")
        url = urlparse(raw if "://" in raw else f"http://{raw}")
        return cls(
            endpoint=url.netloc,
            access_key=access_key,
            secret_key=secret_key,
            use_ssl=url.scheme == "https",
        )


@dataclass(frozen=True)
class LakeLayout:
    """Dónde están Silver y Gold: prefijos s3://bucket o directorios locales."""

    silver_root: str
    gold_root: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> LakeLayout:
        return cls(
            silver_root=env.get("SILVER_ROOT") or f"s3://{env.get('MINIO_BUCKET_SILVER', 'raillytics-silver')}",
            gold_root=env.get("GOLD_ROOT") or f"s3://{env.get('MINIO_BUCKET_GOLD', 'raillytics-gold')}",
        )

    @property
    def uses_s3(self) -> bool:
        return is_s3(self.silver_root) or is_s3(self.gold_root)

    # Cada tabla vive en su propio prefijo. Se lee por glob para admitir tanto
    # el fichero único que escribe DuckDB como los part-*.parquet de Spark.
    def silver_glob(self, table: str) -> str:
        return f"{self.silver_root}/{table}/*.parquet"

    def silver_file(self, table: str) -> str:
        return f"{self.silver_root}/{table}/{table}.parquet"

    def gold_glob(self, table: str) -> str:
        return f"{self.gold_root}/{table}/*.parquet"

    def gold_file(self, table: str) -> str:
        return f"{self.gold_root}/{table}/{table}.parquet"


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def ensure_parent_dir(uri: str) -> None:
    """DuckDB no crea directorios al escribir en disco (en S3 no hacen falta)."""
    if not is_s3(uri):
        Path(uri).parent.mkdir(parents=True, exist_ok=True)


def connect(
    s3: S3Settings | None = None,
    database: str = ":memory:",
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Abre una conexión DuckDB, con acceso a MinIO ya configurado si se pasa s3."""
    con = duckdb.connect(database, read_only=read_only)
    if s3 is not None:
        configure_s3(con, s3)
    return con


def configure_s3(con: duckdb.DuckDBPyConnection, s3: S3Settings, persistent: bool = False) -> None:
    """Carga httpfs y registra las credenciales de MinIO como secret de DuckDB.

    Temporal por defecto (vive lo que la conexión). Con persistent=True se
    guarda en ~/.duckdb/stored_secrets y lo ven todas las conexiones futuras
    del mismo usuario, incluidas las que Superset abre por su cuenta.
    """
    # INSTALL es un no-op si la extensión ya está descargada; en la imagen de
    # Superset se instala al construirla para no depender de internet.
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    scope = "PERSISTENT " if persistent else ""
    con.execute(
        f"""
        CREATE OR REPLACE {scope}SECRET {S3_SECRET_NAME} (
            TYPE s3,
            KEY_ID {_sql_literal(s3.access_key)},
            SECRET {_sql_literal(s3.secret_key)},
            ENDPOINT {_sql_literal(s3.endpoint)},
            URL_STYLE 'path',
            USE_SSL {"true" if s3.use_ssl else "false"}
        )
        """
    )
    logger.info(
        "secret S3 '%s' %s para %s", S3_SECRET_NAME, "persistente" if persistent else "temporal", s3.endpoint
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.gold.lake",
        description="Utilidades DuckDB del Data Lake (MinIO)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "persist-secret",
        help="guarda las credenciales de MinIO (variables MINIO_*) como secret persistente de DuckDB",
    )
    args = parser.parse_args(argv)

    if args.command == "persist-secret":
        s3 = S3Settings.from_env()
        configure_s3(connect(), s3, persistent=True)
        print(f"secret DuckDB '{S3_SECRET_NAME}' guardado para {s3.endpoint}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    raise SystemExit(main())
