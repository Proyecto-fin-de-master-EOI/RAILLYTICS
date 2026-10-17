-- Dim_Estacion: catálogo de estaciones con los atributos
-- que Silver ya trae normalizados. Silver debe ser consistente por
-- estación; el max() solo colapsa las repeticiones.
-- Los nombres los fija config/data_catalog.yml: la clave es `id_estacion`.
-- `codigo_adif` es el código real de la estación, leído de stops.txt del GTFS de Renfe del NAP.
-- Se lee de la ORIGEN porque Silver lleva ahí los atributos, y como están los seis pares en los dos
-- sentidos, toda estación es origen en algún par: el catálogo sale completo igual.
SELECT
    estacion_origen_id          AS id_estacion,
    max(estacion_origen_nombre) AS nombre,
    max(codigo_adif)            AS codigo_adif,
    max(municipio)              AS municipio,
    max(provincia)              AS provincia,
    max(comunidad)              AS comunidad,
    max(latitud)                AS latitud,
    max(longitud)               AS longitud
FROM silver_viajeros_enriquecidos
GROUP BY estacion_origen_id
ORDER BY estacion_origen_id
