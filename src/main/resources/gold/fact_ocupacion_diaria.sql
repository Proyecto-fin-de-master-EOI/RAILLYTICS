-- Fact_Ocupacion_Diaria: coeficiente de ocupación diaria del corredor AVE Madrid–Barcelona.
-- Grano: fecha. Es el objetivo del modelo del Sprint 2 (docs/modelo-ocupacion.md).
--
-- QUÉ ES Y DE DÓNDE SALE, porque esta tabla es MITAD REAL y presentarla sin el caveat sería engañoso:
--
--   ocupacion = viajeros·km del día / plazas·km del día
--
--   numerador    reparto diario de la DTC. El total del TRIMESTRE es real (CNMC), pero el reparto entre
--                los días lo hacen las reglas de config/reglas_demanda.yml. NO es dato observado.
--   denominador  trenes que circularon (NAP) x plazas·km por tren (CNMC). Los trenes están medidos.
--
-- Se mide en viajeros·km / plazas·km y NO en viajeros por plaza: un asiento se vende por tramos (alguien viaja
-- Madrid–Zaragoza y otro Zaragoza–Barcelona en la misma plaza), así que el ratio de cabezas pasa del 100 %
-- legítimamente y no sirve para detectar lo imposible. Medido en km, el 100 % sí es un techo real.
--
-- `plausible = false` marca los días por encima del 100 %. No se borran, se marcan, y hay que saber que
-- 21 de los 25 caen en fin de semana: no es un fallo aleatorio de la fuente, es que el NAP da trenes y no
-- plazas y las composiciones dobles de los días de ocio no se ven. Cualquier agregado debería filtrar por
-- `plausible` o decir que no lo hace.
SELECT
    CAST(date_format(fecha, 'yyyyMMdd') AS INT)   AS fecha_id,
    fecha,
    'Madrid-Barcelona'                            AS corredor,
    'AVE-MAD-BCN'                                 AS linea_id,
    ocupacion,
    plausible,
    -- Los marcadores del calendario con los que se construyó el reparto. Están aquí y no solo en Dim_Fecha
    -- porque son los que explican el valor del día, y quien mire la ocupación los va a querer al lado.
    festivo,
    festivo_solo_madrid,
    festivo_solo_cataluna,
    vispera,
    puente,
    regreso,
    evento,
    junto_a_evento,
    temperatura_media_mad,
    temperatura_media_bcn,
    precipitacion_mm_mad,
    precipitacion_mm_bcn
FROM silver_ml_ocupacion_diaria
ORDER BY fecha
