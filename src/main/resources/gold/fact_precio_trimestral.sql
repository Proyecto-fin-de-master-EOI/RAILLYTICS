-- Fact_Precio_Trimestral: precio medio por trayecto del corredor AVE Madrid–Barcelona por operador y trimestre (CNMC, datos reales).
-- Grano: (anio, trimestre, operador_id). Aquí RENFE es Renfe-AVE y AVLO es Renfe-AVLO (en la demanda va todo dentro de RENFE).
SELECT
    anio,
    trimestre,
    fecha_inicio,
    'AVE-MAD-BCN' AS linea_id,
    operador_id,
    CAST(avg(precio_medio_eur) AS DECIMAL(10, 2)) AS precio_medio_eur
FROM silver_cnmc_precio_trimestral
WHERE trayecto = 'Madrid-Barcelona'
GROUP BY anio, trimestre, fecha_inicio, operador_id
ORDER BY fecha_inicio, operador_id
