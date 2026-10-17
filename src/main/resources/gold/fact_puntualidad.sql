-- Fact_Puntualidad: un servicio (tren) por fila con su retraso en llegada.
-- Los nombres de columna los fija config/data_catalog.yml: retraso_minutos y estado_servicio.
-- es_puntual aplica el umbral en minutos que fija GoldBuilder (UmbralPuntualidadMin, sustituido en ${umbral_puntualidad_min});
-- un servicio cancelado nunca es puntual y no tiene retraso ni hora real.
SELECT
    CAST(date_format(p.fecha, 'yyyyMMdd') AS INT)                       AS fecha_id,
    p.fecha,
    p.linea_id,
    p.operador_id,
    p.estacion_id,
    p.servicio_id,
    p.hora_prevista,
    p.hora_real,
    hour(p.hora_prevista)                                               AS hora,
    p.retraso_minutos,
    p.estado_servicio,
    p.estado_servicio = 'cancelado'                                     AS cancelado,
    p.estado_servicio <> 'cancelado'
        AND p.retraso_minutos <= ${umbral_puntualidad_min}              AS es_puntual,
    p.condicion_meteo,
    p.temperatura_media,
    p.precipitacion_mm
FROM silver_puntualidad_enriquecida p
ORDER BY p.fecha, p.hora_prevista, p.linea_id
