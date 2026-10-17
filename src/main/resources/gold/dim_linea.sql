-- Dim_Linea: catálogo de líneas (tipo de tren, cabeceras y kilometraje).
-- `longitud_km` es la que declara config/data_catalog.yml y sale de los puntos kilométricos de la
-- primera y la última parada, no de un número puesto a mano.
SELECT
    linea_id,
    max(linea_nombre) AS nombre,
    max(tipo_tren)    AS tipo_tren,
    max(origen)       AS origen,
    max(destino)      AS destino,
    max(longitud_km)  AS longitud_km
FROM silver_viajeros_enriquecidos
GROUP BY linea_id
ORDER BY linea_id
