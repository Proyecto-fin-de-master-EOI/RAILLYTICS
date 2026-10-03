-- Fact_Mercado_Trimestral: demanda, oferta e ingresos trimestrales del corredor AVE Madrid–Barcelona por operador (CNMC, datos reales).
-- Grano: (anio, trimestre, operador_id). operador_id incluye TOTAL (la fila del corredor, que solo trae ingresos) y OTRO (empresas aún
-- sin mapear en el Silver); si dos empresas desconocidas coinciden en un trimestre se suman. Avlo va dentro de RENFE: la CNMC no la separa.
-- linea_id enlaza con Dim_Linea (el corredor `Madrid-Barcelona` de la CNMC es la línea AVE-MAD-BCN del proyecto).
SELECT
    anio,
    trimestre,
    fecha_inicio,
    'AVE-MAD-BCN'         AS linea_id,
    operador_id,
    sum(viajeros)         AS viajeros,
    sum(plazas_ofertadas) AS plazas_ofertadas,
    sum(plazas_km)        AS plazas_km,
    sum(tren_km)          AS tren_km,
    sum(viajeros_km)      AS viajeros_km,
    sum(ingresos_eur)     AS ingresos_eur
FROM silver_cnmc_trimestral
WHERE corredor = 'Madrid-Barcelona'
GROUP BY anio, trimestre, fecha_inicio, operador_id
ORDER BY fecha_inicio, operador_id
