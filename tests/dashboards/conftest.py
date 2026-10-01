"""Gold mínimo, en Parquet local, para ejecutar el SQL de los datasets de Superset sin MinIO."""
import duckdb
import pytest

CORREDOR = "AVE-MAD-BCN"


@pytest.fixture
def gold(tmp_path):
    """Dos líneas (la del corredor y una de Cercanías que nunca debe aparecer) y dos operadores.

    Corredor, sábado 3 y lunes 5: Renfe 600 + 2.000 viajeros y Ouigo 400 + 1.000 (total 4.000, 1.000 en fin de semana).
    Cercanías: 9.999 viajeros de Renfe. Servicios del corredor: Renfe puntual (3 min) y Ouigo con 10 min de retraso.
    """
    con = duckdb.connect()

    def tabla(nombre, select):
        destino = tmp_path / nombre
        destino.mkdir(parents=True)
        con.execute(f"COPY ({select}) TO '{(destino / (nombre + '.parquet')).as_posix()}' (FORMAT PARQUET)")

    tabla("dim_linea", f"""SELECT * FROM (VALUES ('{CORREDOR}', 'AVE Madrid – Barcelona', 'AVE', 'Madrid', 'Barcelona'),
        ('C-MAD-C2', 'Cercanías Madrid C-2', 'Cercanías', 'Alcalá', 'Chamartín')) t(linea_id, nombre, tipo_tren, origen, destino)""")
    tabla("dim_operador", """SELECT * FROM (VALUES ('RENFE', 'Renfe', 'Renfe Viajeros', 'Alta velocidad'),
        ('OUIGO', 'Ouigo', 'SNCF Voyageurs España', 'Low cost')) t(operador_id, nombre, empresa, segmento)""")
    tabla("dim_estacion", """SELECT * FROM (VALUES ('MADPA', 'Madrid Puerta de Atocha', 'Madrid', 'Comunidad de Madrid', 40.4, -3.7),
        ('ALCHE', 'Alcalá de Henares', 'Madrid', 'Comunidad de Madrid', 40.5, -3.4)) t(estacion_id, nombre, provincia, comunidad, latitud, longitud)""")
    tabla("dim_fecha", """SELECT * FROM (VALUES (20261003, DATE '2026-10-03', 2026, 4, 10, 'octubre', 6, 'sábado', true, false, NULL::VARCHAR, 'otoño'),
        (20261005, DATE '2026-10-05', 2026, 4, 10, 'octubre', 1, 'lunes', false, false, NULL::VARCHAR, 'otoño'))
        t(fecha_id, fecha, anio, trimestre, mes, nombre_mes, dia_semana, nombre_dia, es_fin_de_semana, es_festivo, festivo_nombre, estacion_anio)""")
    tabla("fact_viajeros", f"""SELECT * FROM (VALUES
        (20261003, DATE '2026-10-03', 'MADPA', '{CORREDOR}', 'RENFE', 600, 15.0, 0.0, 'despejado'),
        (20261003, DATE '2026-10-03', 'MADPA', '{CORREDOR}', 'OUIGO', 400, 15.0, 0.0, 'despejado'),
        (20261005, DATE '2026-10-05', 'MADPA', '{CORREDOR}', 'RENFE', 2000, 14.0, 0.0, 'despejado'),
        (20261005, DATE '2026-10-05', 'MADPA', '{CORREDOR}', 'OUIGO', 1000, 14.0, 0.0, 'despejado'),
        (20261003, DATE '2026-10-03', 'ALCHE', 'C-MAD-C2', 'RENFE', 9999, 15.0, 0.0, 'despejado'))
        t(fecha_id, fecha, estacion_id, linea_id, operador_id, viajeros, temperatura_media, precipitacion_mm, condicion_meteo)""")
    tabla("fact_puntualidad", f"""SELECT * FROM (VALUES
        (20261003, DATE '2026-10-03', '{CORREDOR}', 'RENFE', 'MADPA', 's1', TIMESTAMP '2026-10-03 08:00:00', TIMESTAMP '2026-10-03 08:03:00', 8, 3, 'realizado', false, true, 'despejado', 15.0, 0.0),
        (20261003, DATE '2026-10-03', '{CORREDOR}', 'OUIGO', 'MADPA', 's2', TIMESTAMP '2026-10-03 09:00:00', TIMESTAMP '2026-10-03 09:10:00', 9, 10, 'realizado', false, false, 'despejado', 15.0, 0.0),
        (20261003, DATE '2026-10-03', 'C-MAD-C2', 'RENFE', 'ALCHE', 's3', TIMESTAMP '2026-10-03 09:00:00', TIMESTAMP '2026-10-03 09:30:00', 9, 30, 'realizado', false, false, 'despejado', 15.0, 0.0))
        t(fecha_id, fecha, linea_id, operador_id, estacion_id, servicio_id, hora_prevista, hora_real, hora, retraso_min, estado, cancelado, es_puntual, condicion_meteo, temperatura_media, precipitacion_mm)""")

    def consulta(dataset_sql, select, agrupar=""):
        sql = dataset_sql.replace("s3://raillytics-gold", tmp_path.as_posix())
        return con.execute(f"SELECT {select} FROM ({sql}) t {agrupar}").fetchall()

    return consulta
