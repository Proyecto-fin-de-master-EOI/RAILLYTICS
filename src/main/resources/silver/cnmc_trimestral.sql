-- Silver cnmc_trimestral: indicadores trimestrales del transporte ferroviario de viajeros de la CNMC (LD AV, servicio comercial).
-- Entrada: la vista `entrada` = última foto de l2/cnmc_indicadores (todo string, con los nombres de la cabecera del CSV) y _source_file.
-- Grano: (anio, trimestre, corredor, empresa). Conserva todos los corredores; Gold filtra Madrid-Barcelona.
-- try_cast: con ANSI activado (Spark 4) un texto inválido lanzaría una excepción; así queda NULL y lo cazan los gates (claves)
-- o es «dato no disponible» (volúmenes). La fila `Total` solo trae ingresos y Avlo va dentro de Renfe Viajeros (la CNMC no la separa).
WITH base AS (
    SELECT
        try_cast(substr(`Trimestre`, 1, 4) AS INT)                                  AS anio,
        try_cast(substr(`Trimestre`, 6, 1) AS INT)                                  AS trimestre,
        trim(`Corredor`)                                                            AS corredor,
        trim(`Empresa`)                                                             AS empresa,
        try_cast(NULLIF(trim(`Viajeros (Núm)`), '') AS BIGINT)                      AS viajeros,
        try_cast(NULLIF(trim(`Plazas Ofertadas (Núm)`), '') AS BIGINT)              AS plazas_ofertadas,
        try_cast(NULLIF(trim(`Plazas.km Ofertadas (Plazas.km)`), '') AS BIGINT)     AS plazas_km,
        try_cast(NULLIF(trim(`Tren.km (Tren.km)`), '') AS BIGINT)                   AS tren_km,
        try_cast(NULLIF(trim(`Viajeros.km (Viajeros.km)`), '') AS BIGINT)           AS viajeros_km,
        try_cast(NULLIF(trim(`Ingresos por venta de billetes (€)`), '') AS BIGINT)  AS ingresos_eur,
        _source_file
    FROM entrada
    WHERE trim(`Tipo de producto`) = 'LD AV' AND trim(`Tipo de servicio`) = 'Comercial'
),
mapeada AS (
    SELECT *,
        CASE empresa
            WHEN 'Renfe Viajeros' THEN 'RENFE'
            WHEN 'Iryo'           THEN 'IRYO'
            WHEN 'OUIGO'          THEN 'OUIGO'
            WHEN 'Total'          THEN 'TOTAL'
            ELSE 'OTRO'
        END AS operador_id
    FROM base
),
deduplicada AS (
    SELECT *, row_number() OVER (PARTITION BY anio, trimestre, corredor, empresa ORDER BY _source_file DESC) AS rn
    FROM mapeada
)
SELECT
    anio,
    trimestre,
    CASE WHEN trimestre BETWEEN 1 AND 4 THEN make_date(anio, (trimestre - 1) * 3 + 1, 1) END AS fecha_inicio,
    corredor,
    empresa,
    operador_id,
    viajeros,
    plazas_ofertadas,
    plazas_km,
    tren_km,
    viajeros_km,
    ingresos_eur
FROM deduplicada
WHERE rn = 1
ORDER BY anio, trimestre, corredor, empresa
