"""Vigila config/eventos_corredor.csv, que se mantiene A MANO.

No hay fuente abierta con histórico de eventos con fecha (Madrid publica solo los próximos 100
días; Barcelona, series anuales agregadas), así que el fichero se cura a mano y se amplía a mano
cada año. Estos tests son la red: una fecha mal escrita, una ciudad que la predicción no conoce o
una fila duplicada se detectan aquí y no en mitad de una predicción.
"""
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from raillytics.prediccion.entradas import CONTRATOS, cargar_config

RAIZ = Path(__file__).resolve().parents[2]

# Las únicas ciudades que entiende la predicción (ver CIUDADES en entradas.py). El contrato admite
# ciudad nula para un evento que no es de ninguna de las dos, pero no una ciudad inventada.
CIUDADES = {"MAD", "BCN"}
# La ventana del proyecto: del 1 de junio de 2025 en adelante.
INICIO_VENTANA = date(2025, 6, 1)


@pytest.fixture(scope="module")
def eventos():
    """El fichero leído con la consulta REAL de config/prediccion.yml, no con una propia."""
    sql = cargar_config(RAIZ / "config" / "prediccion.yml")["eventos"]
    assert "muestra_" not in sql, "la consulta de eventos debe leer el fichero real"
    # La consulta lleva la ruta relativa a la raíz del repositorio, igual que al lanzarla con make.
    df = duckdb.connect().execute(f"SET file_search_path = '{RAIZ.as_posix()}'; {sql}").df()
    # DuckDB devuelve Timestamp; la predicción normaliza a date (ver _normalizar_por_origen).
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    return df


def test_el_fichero_cumple_el_contrato_de_la_prediccion(eventos):
    contrato = CONTRATOS["eventos"]

    assert list(eventos.columns) == list(contrato.columnas)
    for columna in contrato.obligatorias:
        assert eventos[columna].notna().all(), f"hay nulos en '{columna}'"


def test_no_hay_eventos_vacios_ni_ciudades_desconocidas(eventos):
    assert eventos["descripcion"].str.strip().ne("").all()
    # NaN sería ciudad nula, que el contrato permite; cualquier otro valor es un error de tecleo.
    ciudades = set(eventos["ciudad"].dropna().unique())
    assert ciudades <= CIUDADES, f"ciudades no soportadas: {sorted(ciudades - CIUDADES)}"


def test_no_se_repite_el_mismo_evento_el_mismo_dia(eventos):
    # Un duplicado no rompe nada, pero el prompt vería el evento dos veces el mismo día.
    repetidos = eventos[eventos.duplicated(["fecha", "descripcion"], keep=False)]
    assert repetidos.empty, f"filas repetidas:\n{repetidos}"


def test_todas_las_fechas_caen_dentro_de_la_ventana_del_proyecto(eventos):
    # Una fecha fuera de la ventana suele ser un año mal teclado (2025 por 2026).
    fechas = eventos["fecha"]
    assert fechas.min() >= INICIO_VENTANA, f"hay eventos antes de la ventana: {fechas.min()}"
    assert fechas.max() <= date.today(), f"hay eventos en el futuro: {fechas.max()}"


def test_estan_los_dos_clasicos_y_las_ferias_que_mueven_el_corredor(eventos):
    """Comprueba que el fichero no se ha quedado vacío ni ha perdido lo que justifica su existencia.

    Son los eventos con efecto claro entre Madrid y Barcelona: si alguno desaparece del fichero,
    la predicción dejaría de verlo sin que nada avisara.
    """
    por_fecha = dict(zip(eventos["fecha"], eventos["descripcion"]))

    assert "Clásico" in por_fecha.get(date(2025, 10, 26), "")   # Bernabéu
    assert "Clásico" in por_fecha.get(date(2026, 5, 10), "")    # Camp Nou
    assert "Mobile World Congress" in por_fecha.get(date(2026, 3, 2), "")
    assert "FITUR" in por_fecha.get(date(2026, 1, 21), "")
