-- Dim_Fecha: una fila por día presente en Silver (viajeros o puntualidad).
-- fecha_id (entero AAAAMMDD) es la clave que usan las tablas de hechos.
-- El festivo llega ya cruzado desde Silver (enriquecimiento con el BOE).
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
    FROM silver_viajeros_enriquecidos
    GROUP BY fecha
)
SELECT
    CAST(strftime(f.fecha, '%Y%m%d') AS INTEGER)                              AS fecha_id,
    f.fecha,
    year(f.fecha)                                                             AS anio,
    quarter(f.fecha)                                                          AS trimestre,
    month(f.fecha)                                                            AS mes,
    ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
     'septiembre', 'octubre', 'noviembre', 'diciembre'][month(f.fecha)]       AS nombre_mes,
    isodow(f.fecha)                                                           AS dia_semana,  -- 1 = lunes ... 7 = domingo
    ['lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'][isodow(f.fecha)]
                                                                              AS nombre_dia,
    isodow(f.fecha) >= 6                                                      AS es_fin_de_semana,
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
