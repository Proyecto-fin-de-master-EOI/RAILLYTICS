-- Dim_Estacion: catálogo de estaciones con los atributos que Silver ya trae
-- normalizados (código, nombre, provincia, comunidad, lat/lon). Silver debe
-- ser consistente por estacion_id; el max() solo colapsa las repeticiones.
SELECT
    estacion_id,
    max(estacion_nombre) AS nombre,
    max(provincia)       AS provincia,
    max(comunidad)       AS comunidad,
    max(latitud)         AS latitud,
    max(longitud)        AS longitud
FROM silver_viajeros_enriquecidos
GROUP BY estacion_id
ORDER BY estacion_id
