-- Fact_Oferta_Diaria: trenes que circulan cada día por corredor y operador, desde el histórico de horarios del NAP (datos reales).
-- Grano: (fecha, corredor, operador_id). Es la ÚNICA serie diaria medida del proyecto, y es con la que se calibró el coeficiente
-- por día de la semana del reparto de la predicción (config/reglas_demanda.yml, `make calibrar`).
--
-- Dos avisos que hay que arrastrar a cualquier lectura de esta tabla:
--   1. El NAP da TRENES, no plazas. Una composición doble cuenta como un tren, así que esto es servicio y no capacidad.
--      Es la razón de que la ocupación del fin de semana salga alta (ver docs/modelo-ocupacion.md).
--   2. Iryo no publica horarios en el NAP, así que la oferta cubre Renfe y OUIGO pero NO el corredor completo.
--      `cobertura_completa` marca los días en que están todos los operadores que sí publican: un día sin uno de ellos no es
--      menos servicio, es dato incompleto, y contarlo finge un recorte que no existió.
--
-- `trenes_todos` es el total del snapshot antes de filtrar por corredor: sirve para ver cuánto del fichero se usa.
SELECT
    CAST(date_format(fecha, 'yyyyMMdd') AS INT)   AS fecha_id,
    fecha,
    corredor,
    CASE upper(operador)
        WHEN 'RENFE' THEN 'RENFE'
        WHEN 'OUIGO' THEN 'OUIGO'
        ELSE 'OTRO'
    END                                           AS operador_id,
    CASE
        WHEN corredor = 'Madrid-Barcelona' THEN 'AVE-MAD-BCN'
        ELSE NULL
    END                                           AS linea_id,
    sum(trenes)                                   AS trenes,
    max(trenes_todos)                             AS trenes_todos,
    bool_and(cobertura_completa)                  AS cobertura_completa,
    max(snapshot)                                 AS snapshot
FROM silver_oferta_diaria
GROUP BY fecha, corredor, CASE upper(operador)
        WHEN 'RENFE' THEN 'RENFE'
        WHEN 'OUIGO' THEN 'OUIGO'
        ELSE 'OTRO'
    END
ORDER BY fecha, corredor, operador_id
