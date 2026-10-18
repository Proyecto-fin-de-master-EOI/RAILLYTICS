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

    tabla("dim_linea", f"""SELECT * FROM (VALUES ('{CORREDOR}', 'AVE Madrid – Barcelona', 'AVE', 'Madrid', 'Barcelona', 621.0),
        ('C-MAD-C2', 'Cercanías Madrid C-2', 'Cercanías', 'Alcalá', 'Chamartín', 36.0)) t(linea_id, nombre, tipo_tren, origen, destino, longitud_km)""")
    tabla("dim_operador", """SELECT * FROM (VALUES ('RENFE', 'Renfe', 'Renfe Viajeros', 'Alta velocidad'),
        ('OUIGO', 'Ouigo', 'SNCF Voyageurs España', 'Low cost')) t(operador_id, nombre, empresa, segmento)""")
    # La clave de la dimensión es `id_estacion`, que es como la declara config/data_catalog.yml.
    tabla("dim_estacion", """SELECT * FROM (VALUES ('MADPA', 'Madrid Puerta de Atocha', 'Madrid', 'Comunidad de Madrid', 40.4, -3.7),
        ('BCNSA', 'Barcelona Sants', 'Barcelona', 'Cataluña', 41.4, 2.1),
        ('ALCHE', 'Alcalá de Henares', 'Madrid', 'Comunidad de Madrid', 40.5, -3.4)) t(id_estacion, nombre, provincia, comunidad, latitud, longitud)""")
    tabla("dim_fecha", """SELECT * FROM (VALUES (20261003, DATE '2026-10-03', 2026, 4, 10, 3, 'octubre', 6, 'sábado', true, false, NULL::VARCHAR, 'otoño'),
        (20261005, DATE '2026-10-05', 2026, 4, 10, 5, 'octubre', 1, 'lunes', false, false, NULL::VARCHAR, 'otoño'))
        t(fecha_id, fecha, anio, trimestre, mes, dia, nombre_mes, dia_semana, nombre_dia, es_fin_semana, es_festivo, festivo_nombre, estacion_anio)""")
    # El grano es el par origen-destino, y `viajeros_km` es viajeros × distancia_km.
    tabla("fact_viajeros", f"""SELECT * FROM (VALUES
        (20261003, DATE '2026-10-03', 'MADPA', 'BCNSA', 621.0, '{CORREDOR}', 'RENFE', 600, 372600.0, 15.0, 0.0, 'despejado'),
        (20261003, DATE '2026-10-03', 'MADPA', 'BCNSA', 621.0, '{CORREDOR}', 'OUIGO', 400, 248400.0, 15.0, 0.0, 'despejado'),
        (20261005, DATE '2026-10-05', 'MADPA', 'BCNSA', 621.0, '{CORREDOR}', 'RENFE', 2000, 1242000.0, 14.0, 0.0, 'despejado'),
        (20261005, DATE '2026-10-05', 'MADPA', 'BCNSA', 621.0, '{CORREDOR}', 'OUIGO', 1000, 621000.0, 14.0, 0.0, 'despejado'),
        (20261003, DATE '2026-10-03', 'ALCHE', 'MADPA', 36.0, 'C-MAD-C2', 'RENFE', 9999, 359964.0, 15.0, 0.0, 'despejado'))
        t(fecha_id, fecha, estacion_origen_id, estacion_destino_id, distancia_km, linea_id, operador_id, viajeros, viajeros_km, temperatura_media, precipitacion_mm, condicion_meteo)""")
    tabla("fact_puntualidad", f"""SELECT * FROM (VALUES
        (20261003, DATE '2026-10-03', '{CORREDOR}', 'RENFE', 'MADPA', 's1', TIMESTAMP '2026-10-03 08:00:00', TIMESTAMP '2026-10-03 08:03:00', 8, 3, 'realizado', false, true, 'despejado', 15.0, 0.0),
        (20261003, DATE '2026-10-03', '{CORREDOR}', 'OUIGO', 'MADPA', 's2', TIMESTAMP '2026-10-03 09:00:00', TIMESTAMP '2026-10-03 09:10:00', 9, 10, 'realizado', false, false, 'despejado', 15.0, 0.0),
        (20261003, DATE '2026-10-03', 'C-MAD-C2', 'RENFE', 'ALCHE', 's3', TIMESTAMP '2026-10-03 09:00:00', TIMESTAMP '2026-10-03 09:30:00', 9, 30, 'realizado', false, false, 'despejado', 15.0, 0.0))
        t(fecha_id, fecha, linea_id, operador_id, estacion_id, servicio_id, hora_prevista, hora_real, hora, retraso_minutos, estado_servicio, cancelado, es_puntual, condicion_meteo, temperatura_media, precipitacion_mm)""")

    # Mercado (CNMC): 7 trimestres seguidos (2024-T4 … 2026-T2), Renfe y Ouigo + la fila TOTAL con ingresos. Con 7 trimestres
    # hay variación interanual (LAG 4) y un único punto de backtest (LAG 6) en 2026-T2.
    trimestres = [(2024, 4), (2025, 1), (2025, 2), (2025, 3), (2025, 4), (2026, 1), (2026, 2)]
    renfe = [900, 1000, 1100, 1200, 1000, 800, 880]
    ouigo = [300, 320, 350, 360, 330, 290, 310]
    ingresos = [10000, 11000, 12000, 13000, 12000, 9000, 10000]
    filas = []
    for (anio, t), r, o, i in zip(trimestres, renfe, ouigo, ingresos):
        inicio = f"DATE '{anio}-{(t - 1) * 3 + 1:02d}-01'"
        filas += [
            f"({anio}, {t}, {inicio}, '{CORREDOR}', 'RENFE', {r}, 1000, {r * 100}, {r // 2}, {r * 3}, NULL)",
            f"({anio}, {t}, {inicio}, '{CORREDOR}', 'OUIGO', {o}, 400, {o * 100}, {o // 2}, {o * 3}, NULL)",
            f"({anio}, {t}, {inicio}, '{CORREDOR}', 'TOTAL', NULL, NULL, NULL, NULL, NULL, {i})",
        ]
    tabla("fact_mercado_trimestral", "SELECT * FROM (VALUES " + ", ".join(filas) + ") t(anio, trimestre, fecha_inicio, linea_id, operador_id, viajeros, plazas_ofertadas, plazas_km, tren_km, viajeros_km, ingresos_eur)")
    tabla("fact_precio_trimestral", f"""SELECT * FROM (VALUES
        (2026, 1, DATE '2026-01-01', '{CORREDOR}', 'RENFE', 70.00), (2026, 1, DATE '2026-01-01', '{CORREDOR}', 'OUIGO', 52.00), (2026, 1, DATE '2026-01-01', '{CORREDOR}', 'TOTAL', 60.00),
        (2026, 2, DATE '2026-04-01', '{CORREDOR}', 'RENFE', 72.00), (2026, 2, DATE '2026-04-01', '{CORREDOR}', 'OUIGO', 54.00), (2026, 2, DATE '2026-04-01', '{CORREDOR}', 'TOTAL', 62.00)
        ) t(anio, trimestre, fecha_inicio, linea_id, operador_id, precio_medio_eur)""")
    tabla("fact_precio_mensual", f"""SELECT * FROM (VALUES
        (2026, 5, DATE '2026-05-01', '{CORREDOR}', 'RENFE', 71.00), (2026, 5, DATE '2026-05-01', '{CORREDOR}', 'AVLO', 40.00), (2026, 5, DATE '2026-05-01', '{CORREDOR}', 'TOTAL', 61.00),
        (2026, 6, DATE '2026-06-01', '{CORREDOR}', 'RENFE', 73.00), (2026, 6, DATE '2026-06-01', '{CORREDOR}', 'AVLO', 41.00), (2026, 6, DATE '2026-06-01', '{CORREDOR}', 'TOTAL', 63.00)
        ) t(anio, mes, fecha_inicio, linea_id, operador_id, precio_medio_eur)""")

    def consulta(dataset_sql, select, agrupar=""):
        sql = dataset_sql.replace("s3://raillytics-gold", tmp_path.as_posix())
        return con.execute(f"SELECT {select} FROM ({sql}) t {agrupar}").fetchall()

    return consulta
