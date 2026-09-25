-- Fact_Puntualidad: un servicio (tren) por fila con su retraso en llegada.
-- es_puntual aplica el umbral en minutos que fija build.py
-- (PUNTUALIDAD_UMBRAL_MIN); un servicio cancelado nunca es puntual y no
-- tiene retraso ni hora real.
SELECT
    CAST(strftime(p.fecha, '%Y%m%d') AS INTEGER)                        AS fecha_id,
    p.fecha,
    p.linea_id,
    p.estacion_id,
    p.servicio_id,
    p.hora_prevista,
    p.hora_real,
    hour(p.hora_prevista)                                               AS hora,
    p.retraso_min,
    p.estado,
    p.estado = 'cancelado'                                              AS cancelado,
    p.estado <> 'cancelado'
        AND p.retraso_min <= getvariable('umbral_puntualidad_min')      AS es_puntual,
    p.condicion_meteo,
    p.temperatura_media,
    p.precipitacion_mm
FROM silver_puntualidad_enriquecida p
ORDER BY p.fecha, p.hora_prevista, p.linea_id
