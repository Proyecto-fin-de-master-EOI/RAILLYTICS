-- Dim_Fecha: una fila por día presente en Silver (viajeros o puntualidad); fecha_id
-- (entero AAAAMMDD) es la clave que usan las tablas de hechos. El festivo
-- llega ya cruzado desde Silver (enriquecimiento con el BOE): se toma de las
-- dos tablas, porque un día que solo tenga servicios (sin viajeros) también
-- puede ser festivo; el nombre solo lo trae viajeros.
WITH fechas AS (
    SELECT fecha FROM silver_viajeros_enriquecidos
    UNION
    SELECT fecha FROM silver_puntualidad_enriquecida
),
festivos AS (
    SELECT
        fecha,
        bool_or(es_festivo)  AS es_festivo,
        max(festivo_nombre)  AS festivo_nombre
    FROM (
        SELECT fecha, es_festivo, festivo_nombre FROM silver_viajeros_enriquecidos
        UNION ALL
        SELECT fecha, es_festivo, CAST(NULL AS STRING) AS festivo_nombre FROM silver_puntualidad_enriquecida
    ) f
    GROUP BY fecha
)
SELECT
    CAST(date_format(f.fecha, 'yyyyMMdd') AS INT)                             AS fecha_id,
    f.fecha,
    year(f.fecha)                                                             AS anio,
    quarter(f.fecha)                                                          AS trimestre,
    month(f.fecha)                                                            AS mes,
    element_at(array('enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
                     'septiembre', 'octubre', 'noviembre', 'diciembre'), month(f.fecha)) AS nombre_mes,
    weekday(f.fecha) + 1                                                      AS dia_semana,  -- 1 = lunes ... 7 = domingo
    element_at(array('lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'),
               weekday(f.fecha) + 1)                                          AS nombre_dia,
    weekday(f.fecha) >= 5                                                     AS es_fin_de_semana,
    coalesce(h.es_festivo, false)                                             AS es_festivo,
    h.festivo_nombre,
    CASE
        WHEN month(f.fecha) IN (12, 1, 2) THEN 'invierno'
        WHEN month(f.fecha) IN (3, 4, 5)  THEN 'primavera'
        WHEN month(f.fecha) IN (6, 7, 8)  THEN 'verano'
        ELSE 'otoño'
    END                                                                       AS estacion_anio
FROM fechas f
LEFT JOIN festivos h USING (fecha)
ORDER BY f.fecha
