-- Solo para SilverBuilderSpec: una tabla en modo incremental que expone la fecha de ingesta de cada fichero.
SELECT `Trimestre` AS trimestre, `Viajeros (Núm)` AS viajeros, _fecha_ingesta FROM entrada
