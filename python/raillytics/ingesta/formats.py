# Formatos admitidos en config/data_sources.yml. Debe coincidir con
# SourceFormat.scala (src/main/scala/raillytics/ingesta/formats/), que es
# quien sabe leerlos en L2:
#   csv   fichero de texto con cabecera
#   json  un único documento JSON (p. ej. GTFS-RT), no JSON-Lines
#   zip   archivo ZIP cuyos miembros son CSV (p. ej. un GTFS estático: stops.txt,
#         routes.txt...); L2 lo convierte en un Parquet por miembro
SUPPORTED_FORMATS = {"csv", "json", "zip"}
