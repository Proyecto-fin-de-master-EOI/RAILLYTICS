-- Silver cnmc_precio_trimestral: precio medio trimestral por trayecto y empresa de la CNMC (LD AV, servicio comercial).
-- Entrada: la vista `entrada` = última foto de l2/cnmc_precio_trimestral (todo string). Grano: (anio, trimestre, trayecto, empresa).
-- Los precios traen coma decimal ('62,13'). Aquí Renfe-AVE y Renfe-AVLO son operadores distintos (en la demanda va todo en Renfe Viajeros).
WITH base AS (
    SELECT
        try_cast(substr(`Trimestre`, 1, 4) AS INT)                                  AS anio,
        try_cast(substr(`Trimestre`, 6, 1) AS INT)                                  AS trimestre,
        trim(`Trayecto`)                                                            AS trayecto,
        trim(`Empresa`)                                                             AS empresa,
        try_cast(regexp_replace(trim(`Precio (€)`), ',', '.') AS DECIMAL(10, 2))    AS precio_medio_eur,
        _source_file
    FROM entrada
    WHERE trim(`Tipo de producto`) = 'LD AV' AND trim(`Tipo de servicio`) = 'Comercial'
),
mapeada AS (
    SELECT *,
        CASE empresa
            WHEN 'Renfe-AVE'  THEN 'RENFE'
            WHEN 'Renfe-AVLO' THEN 'AVLO'
            WHEN 'Iryo'       THEN 'IRYO'
            WHEN 'OUIGO'      THEN 'OUIGO'
            WHEN 'Total'      THEN 'TOTAL'
            ELSE 'OTRO'
        END AS operador_id
    FROM base
),
deduplicada AS (
    SELECT *, row_number() OVER (PARTITION BY anio, trimestre, trayecto, empresa ORDER BY _source_file DESC) AS rn
    FROM mapeada
)
SELECT
    anio,
    trimestre,
    CASE WHEN trimestre BETWEEN 1 AND 4 THEN make_date(anio, (trimestre - 1) * 3 + 1, 1) END AS fecha_inicio,
    trayecto,
    empresa,
    operador_id,
    precio_medio_eur
FROM deduplicada
WHERE rn = 1
ORDER BY anio, trimestre, trayecto, empresa
