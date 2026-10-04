# Formatos admitidos en config/data_sources.yml. Debe coincidir con
# SourceFormat.scala (src/main/scala/raillytics/ingesta/formats/), que es
# quien sabe leerlos en L2:
#   csv   fichero de texto con cabecera
#   json  un único documento JSON (p. ej. GTFS-RT), no JSON-Lines
#   zip   archivo ZIP cuyos miembros son CSV (p. ej. un GTFS estático: stops.txt,
#         routes.txt...); L2 lo convierte en un Parquet por miembro
SUPPORTED_FORMATS = {"csv", "json", "zip"}


# Claves permitidas de cada fuente en config/data_sources.yml. Deben coincidir con
# DataSourceConfig.scala (src/main/scala/raillytics/ingesta/config/): una clave
# desconocida es un error en los dos lados (un error tipográfico no se ignora en silencio).
CLAVES_FUENTE = {"id", "name", "url", "format", "options", "checks", "silver", "downloader", "auth"}
# `options`: cómo se lee un fichero csv (Python al validarlo, L2 con Spark).
OPCIONES_LECTURA = {"delimiter", "encoding"}
# `checks`: reglas de la descarga sobre un csv (solo las aplica Python; Scala valida que la clave exista).
CHECKS_DESCARGA = {"min_bytes", "min_filas", "columnas"}
# `silver`: cómo reconstruye `make 04_silver` cada tabla (snapshot: la última foto la sustituye; incremental: se añade).
MODOS_SILVER = {"snapshot", "incremental"}
# `downloader`: quién sabe traer la fuente. Por defecto `http`, una descarga directa de `url`.
# Las fuentes cuya adquisición no es un GET —AEMET parte el histórico en tramos de 15 días y
# responde con un enlace temporal; el NAP obliga a listar snapshots, elegir uno y seguir una URL
# firmada que caduca— aportan el suyo en python/raillytics/ingesta/downloaders.py en vez de
# convertir el YAML en un lenguaje de plantillas. Solo lo usa Python; Scala valida que la clave exista.
DESCARGADORES = {"http", "aemet", "nap"}
# `auth`: de dónde sale la credencial. `env` es el NOMBRE de la variable de entorno, nunca el
# secreto: config/data_sources.yml está versionado. `header` es opcional y solo lo necesitan los
# descargadores que mandan la clave por cabecera.
OPCIONES_AUTH = {"env", "header"}
