-- Fact_Viajeros: viajeros diarios por estación y línea.
-- Grano: (fecha, estación, línea). Las variables meteorológicas del día
-- viajan con el hecho (el cruce con AEMET se hace en Silver); fecha se
-- conserva junto a fecha_id para filtrar por rango sin pasar por Dim_Fecha.
SELECT
    CAST(date_format(v.fecha, 'yyyyMMdd') AS INT) AS fecha_id,
    v.fecha,
    v.estacion_id,
    v.linea_id,
    v.viajeros,
    v.temperatura_media,
    v.precipitacion_mm,
    v.condicion_meteo
FROM silver_viajeros_enriquecidos v
ORDER BY v.fecha, v.linea_id, v.estacion_id
