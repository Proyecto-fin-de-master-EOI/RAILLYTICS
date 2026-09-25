# Imagen de Superset para Raillytics: la oficial no trae ni el driver de DuckDB
# (con el que Superset consulta la capa Gold en MinIO) ni el de Postgres (base
# de metadatos de Superset), así que se añaden aquí. Se construye desde
# docker-compose.yml (servicio superset / superset-init); no hace falta
# construirla a mano.
FROM apache/superset:6.1.0

USER root
# El venv de la imagen (/app/.venv) no incluye pip: la propia imagen instala
# sus dependencias con uv, así que hacemos lo mismo apuntando a ese venv.
# duckdb va fijado a la misma versión que requirements.txt: la extensión
# httpfs que se instala abajo es específica de cada versión de DuckDB.
RUN uv pip install --no-cache-dir --python /app/.venv/bin/python \
      "duckdb==1.5.5" \
      "duckdb-engine==0.17.0" \
      "psycopg2-binary~=2.9"

USER superset
# httpfs es la extensión con la que DuckDB lee s3:// (MinIO). Se descarga en
# tiempo de build al HOME del usuario superset (~/.duckdb/extensions), que es
# donde DuckDB la busca en runtime: así `LOAD httpfs` funciona sin internet y
# cada conexión de Superset arranca al instante.
RUN /app/.venv/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
