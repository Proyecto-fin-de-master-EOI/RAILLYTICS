-- Fact_Viajeros: viajes diarios por par origen-destino, línea y operador.
-- Grano: (fecha, estación de origen, estación de destino, línea, operador), que es el que declara
-- config/data_catalog.yml. Antes había una fila por estación, y eso no era un número de viajes: el
-- que va de Madrid a Barcelona aparecía en las dos puntas, así que la suma del día no cuadraba con
-- nada. Con el par, sum(viajeros) de un día ES el número de viajes del corredor.
-- `distancia_km` sale de los puntos kilométricos de las dos paradas (una resta) y con ella
-- `viajeros_km` se calcula DESDE ABAJO, que es lo que permite contrastarlo con la CNMC.
-- Las variables meteorológicas del día viajan con el hecho (el cruce con AEMET se hace en Silver, y
-- es la meteo de la provincia de ORIGEN); fecha se conserva junto a fecha_id para filtrar por rango
-- sin pasar por Dim_Fecha.
SELECT
    CAST(date_format(v.fecha, 'yyyyMMdd') AS INT) AS fecha_id,
    v.fecha,
    v.estacion_origen_id,
    v.estacion_destino_id,
    v.distancia_km,
    v.linea_id,
    v.operador_id,
    v.viajeros,
    v.viajeros_km,
    v.temperatura_media,
    v.precipitacion_mm,
    v.condicion_meteo
FROM silver_viajeros_enriquecidos v
ORDER BY v.fecha, v.linea_id, v.estacion_origen_id, v.estacion_destino_id, v.operador_id
