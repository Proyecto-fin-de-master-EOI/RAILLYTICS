-- Dim_Linea: catálogo de líneas (tipo de tren, origen y destino).
SELECT
    linea_id,
    max(linea_nombre) AS nombre,
    max(tipo_tren)    AS tipo_tren,
    max(origen)       AS origen,
    max(destino)      AS destino
FROM silver_viajeros_enriquecidos
GROUP BY linea_id
ORDER BY linea_id
