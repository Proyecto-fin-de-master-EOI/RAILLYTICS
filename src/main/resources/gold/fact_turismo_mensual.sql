-- Fact_Turismo_Mensual: turistas que se mueven de Madrid a cada provincia destino, mes a mes (INE, datos reales).
-- Grano: (anio, mes, origen, destino). Operación TMOV, tabla 53267: el INE lo estima con la POSICIÓN DE LOS
-- TELÉFONOS MÓVILES, así que es un conteo y no una encuesta. Serie mensual desde julio de 2019.
--
-- PARA QUÉ ESTÁ: es el único dato de nivel MENSUAL del proyecto. CNMC solo publica trimestres, así que el
-- reparto diario de la predicción trataba enero y marzo como el mismo nivel. De esta tabla sale `base_mes`
-- de config/reglas_demanda.yml (`make perfil-mensual`).
--
-- QUÉ MIDE Y QUÉ NO, y esto no es un detalle: son viajes en TODOS los medios de transporte, no solo en tren,
-- y «turista» en TMOV implica pernoctación, así que la ida y vuelta en el día —mucho del negocio en este
-- corredor— no entra. Por eso se usa la FORMA de la serie (qué peso tiene cada mes dentro de su trimestre) y
-- NO su nivel. Presentar estas cifras como viajeros de tren sería desnaturalizar el dato, que es justo lo que
-- prohíbe la licencia del INE (CC BY 4.0, ver docs/licencias.md).
--
-- NO enlaza con Dim_Fecha: su grano es el mes y el de Dim_Fecha el día. Lleva sus propios atributos de mes.
--
-- `tipo_dato` distingue definitivo, provisional y avance: el INE marca los meses recientes como provisionales.
SELECT
    year(fecha)                                   AS anio,
    month(fecha)                                  AS mes,
    fecha                                         AS primer_dia_mes,
    element_at(array('enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto',
                     'septiembre', 'octubre', 'noviembre', 'diciembre'), month(fecha)) AS nombre_mes,
    quarter(fecha)                                AS trimestre,
    origen,
    destino,
    turistas,
    tipo_dato,
    -- Cuota del mes dentro de su trimestre: es la medida que de verdad se usa, y tenerla calculada aquí evita
    -- que cada gráfica la rehaga mal. Solo tiene sentido en trimestres con sus tres meses, así que los
    -- incompletos van a NULL en vez de dar una cuota inflada (con enero y febrero pero sin marzo, enero
    -- saldría con el 50 %).
    CASE
        WHEN count(*) OVER (PARTITION BY origen, destino, year(fecha), quarter(fecha)) = 3
        THEN turistas / sum(turistas) OVER (PARTITION BY origen, destino, year(fecha), quarter(fecha))
        ELSE NULL
    END                                           AS cuota_del_trimestre
FROM silver_turismo_mensual
ORDER BY origen, destino, fecha
