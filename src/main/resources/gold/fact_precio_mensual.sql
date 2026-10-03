-- Fact_Precio_Mensual: precio medio por trayecto del corredor AVE Madrid–Barcelona por operador y mes (CNMC, datos reales).
-- Grano: (anio, mes, operador_id). Mismo criterio de operadores que Fact_Precio_Trimestral.
SELECT
    anio,
    mes,
    fecha_inicio,
    'AVE-MAD-BCN' AS linea_id,
    operador_id,
    CAST(avg(precio_medio_eur) AS DECIMAL(10, 2)) AS precio_medio_eur
FROM silver_cnmc_precio_mensual
WHERE trayecto = 'Madrid-Barcelona'
GROUP BY anio, mes, fecha_inicio, operador_id
ORDER BY fecha_inicio, operador_id
