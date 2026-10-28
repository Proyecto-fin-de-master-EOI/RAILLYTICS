-- Dim_Fecha, versión del grupo «corredor»: la misma dimensión que dim_fecha.sql pero con dos arreglos que
-- solo son posibles cuando están las tablas Silver reales. Escribe la MISMA tabla Gold (dim_fecha), así que
-- los hechos del modelo núcleo siguen enlazando igual.
--
-- QUÉ ARREGLA respecto a dim_fecha.sql:
--
--   1. EL RANGO. La versión del núcleo saca los días de las dos tablas Silver SINTÉTICAS, así que los hechos
--      reales (oferta del NAP desde junio de 2025, ocupación de 2026) tenían fechas que no existían en la
--      dimensión y se quedaban sin atributos al enlazar.
--   2. EL FESTIVO. La versión del núcleo lo toma de la marca `es_festivo` del Silver sintético, que la genera
--      el propio generador. Aquí sale del calendario laboral del BOE (silver_festivos), que es dato real.
--      Eso importa: el festivo de Gold era inventado teniendo la tabla de verdad al lado.
--
-- Y añade lo que ninguna de las dos tenía: el ámbito del festivo y la meteo observada de las dos cabeceras.
-- El ámbito se saca del nombre, porque es donde lo deja la ingesta del BOE: «Jueves Santo (Madrid)» es
-- festivo en Madrid y laborable en Cataluña, y para un corredor que une las dos ciudades eso cambia el viaje.
-- El fichero no se llama como su tabla, asi que lo declara para el grafo de linaje:
-- tabla: dim_fecha
WITH fechas AS (
    SELECT fecha FROM silver_viajeros_enriquecidos
    UNION
    SELECT fecha FROM silver_puntualidad_enriquecida
    UNION
    SELECT fecha FROM silver_oferta_diaria
    UNION
    SELECT fecha FROM silver_ml_ocupacion_diaria
    UNION
    SELECT fecha FROM silver_meteo
    UNION
    -- Los festivos del BOE de un año que todavía no tenga ningún dato operativo: así un calendario cargado
    -- por adelantado no desaparece de la dimensión.
    SELECT fecha FROM silver_festivos
),
festivo AS (
    SELECT
        fecha,
        nombre AS festivo_nombre,
        CASE
            WHEN nombre LIKE '%(Madrid)%'   THEN 'Madrid'
            WHEN nombre LIKE '%(Cataluña)%' THEN 'Cataluña'
            ELSE 'nacional'
        END AS festivo_ambito
    FROM silver_festivos
),
clima AS (
    SELECT
        fecha,
        max(CASE WHEN ciudad = 'MAD' THEN temperatura_media END) AS temperatura_media_mad,
        max(CASE WHEN ciudad = 'BCN' THEN temperatura_media END) AS temperatura_media_bcn,
        max(CASE WHEN ciudad = 'MAD' THEN precipitacion_mm  END) AS precipitacion_mm_mad,
        max(CASE WHEN ciudad = 'BCN' THEN precipitacion_mm  END) AS precipitacion_mm_bcn
    FROM silver_meteo
    GROUP BY fecha
)
SELECT
    CAST(date_format(f.fecha, 'yyyyMMdd') AS INT)                             AS fecha_id,
    f.fecha,
    year(f.fecha)                                                             AS anio,
    quarter(f.fecha)                                                          AS trimestre,
    month(f.fecha)                                                            AS mes,
    day(f.fecha)                                                              AS dia,
    element_at(array('enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
                     'septiembre', 'octubre', 'noviembre', 'diciembre'), month(f.fecha)) AS nombre_mes,
    weekday(f.fecha) + 1                                                      AS dia_semana,  -- 1 = lunes ... 7 = domingo
    element_at(array('lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'),
               weekday(f.fecha) + 1)                                          AS nombre_dia,
    weekday(f.fecha) >= 5                                                     AS es_fin_semana,
    h.fecha IS NOT NULL                                                       AS es_festivo,
    h.festivo_nombre,
    h.festivo_ambito,
    CASE
        WHEN month(f.fecha) IN (12, 1, 2) THEN 'invierno'
        WHEN month(f.fecha) IN (3, 4, 5)  THEN 'primavera'
        WHEN month(f.fecha) IN (6, 7, 8)  THEN 'verano'
        ELSE 'otoño'
    END                                                                       AS estacion_anio,
    c.temperatura_media_mad,
    c.temperatura_media_bcn,
    c.precipitacion_mm_mad,
    c.precipitacion_mm_bcn
FROM fechas f
LEFT JOIN festivo h USING (fecha)
LEFT JOIN clima c USING (fecha)
ORDER BY f.fecha
