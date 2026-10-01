-- Dim_Operador: catálogo de operadores del corredor (empresa y segmento: alta velocidad o low cost).
SELECT
    operador_id,
    max(operador_nombre)   AS nombre,
    max(operador_empresa)  AS empresa,
    max(operador_segmento) AS segmento
FROM silver_viajeros_enriquecidos
GROUP BY operador_id
ORDER BY operador_id
