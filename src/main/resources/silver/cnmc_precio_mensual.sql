-- Silver cnmc_precio_mensual: precio medio mensual por trayecto y empresa de la CNMC (LD AV, servicio comercial).
-- Entrada: la vista `entrada` = última foto de l2/cnmc_precio_mensual (todo string; `Mes` = '2026-06'). Grano: (anio, mes, trayecto, empresa).
WITH base AS (
    SELECT
        try_cast(substr(`Mes`, 1, 4) AS INT)                                        AS anio,
        try_cast(substr(`Mes`, 6, 2) AS INT)                                        AS mes,
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
    SELECT *, row_number() OVER (PARTITION BY anio, mes, trayecto, empresa ORDER BY _source_file DESC) AS rn
    FROM mapeada
)
SELECT
    anio,
    mes,
    CASE WHEN mes BETWEEN 1 AND 12 THEN make_date(anio, mes, 1) END AS fecha_inicio,
    trayecto,
    empresa,
    operador_id,
    precio_medio_eur
FROM deduplicada
WHERE rn = 1
ORDER BY anio, mes, trayecto, empresa
