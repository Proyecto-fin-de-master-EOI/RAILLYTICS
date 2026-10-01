import csv
import io
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from raillytics.prediccion.calendario import construir_calendario
from raillytics.prediccion.normalizar import IndiceDia, normalizar_indices, repartir
from raillytics.prediccion.salida import (
    COLUMNAS_CSV,
    CORREDOR,
    construir_dataframe,
    escribir_csv,
    formatear_resumen,
    resumen_coherencia,
    ruta_csv,
)
from raillytics.prediccion.trimestre import Trimestre

T4 = Trimestre(2026, 4)
DIAS = T4.dias()
AHORA = datetime(2026, 10, 1, 16, 51, 0)


def _df(motivo="laborable", total=1_320_000):
    valores = [1.0 + 0.1 * (i % 3) for i in range(len(DIAS))]
    indices = [IndiceDia(d, v, motivo) for d, v in zip(DIAS, valores)]
    return construir_dataframe(
        DIAS, indices, repartir(valores, total), normalizar_indices(valores),
        trimestre=T4, modelo="mistral-nemo", version_prompt="demanda_v1", run_id="run-1", generado_en=AHORA,
    )


def test_el_dataframe_sigue_el_contrato_del_spec():
    df = _df()

    assert tuple(df.columns) == COLUMNAS_CSV == (
        "fecha", "corredor", "viajeros_previstos", "indice", "motivo", "trimestre",
        "modelo", "version_prompt", "run_id", "generado_en",
    )
    assert len(df) == 92 and df["fecha"].iloc[0] == "2026-10-01" and df["fecha"].iloc[-1] == "2026-12-31"
    assert set(df["corredor"]) == {CORREDOR} == {"AVE-MAD-BCN"}
    assert df["viajeros_previstos"].sum() == 1_320_000
    assert set(df["trimestre"]) == {"2026-T4"} and set(df["generado_en"]) == {"2026-10-01T16:51:00Z"}
    assert set(df["modelo"]) == {"mistral-nemo"} and set(df["run_id"]) == {"run-1"}


def test_los_indices_deben_corresponder_dia_a_dia():
    desordenados = [IndiceDia(d, 1.0, "x") for d in reversed(DIAS)]

    with pytest.raises(ValueError, match="no coinciden"):
        construir_dataframe(
            DIAS, desordenados, [1] * 92, [1.0] * 92,
            trimestre=T4, modelo="m", version_prompt="v", run_id="r", generado_en=AHORA,
        )


def test_la_ruta_agrupa_por_corredor_y_trimestre(tmp_path):
    ruta = ruta_csv(tmp_path, T4, "demanda_v1", AHORA)

    assert ruta == tmp_path / "AVE-MAD-BCN" / "2026-T4" / "demanda_diaria_2026-T4_demanda_v1_20261001T165100Z.csv"


def test_escribir_csv_crea_el_directorio_y_no_deja_temporales(tmp_path):
    ruta = escribir_csv(_df(), tmp_path / "salida", T4, "demanda_v1", AHORA)

    assert ruta.is_file()
    assert [p.name for p in ruta.parent.iterdir()] == [ruta.name]
    lineas = ruta.read_text(encoding="utf-8").splitlines()
    assert lineas[0] == ",".join(COLUMNAS_CSV) and len(lineas) == 93


def test_el_indice_se_escribe_con_seis_decimales_y_punto_decimal(tmp_path):
    ruta = escribir_csv(_df(), tmp_path, T4, "demanda_v1", AHORA)

    fila = next(csv.DictReader(io.StringIO(ruta.read_text(encoding="utf-8"))))
    assert len(fila["indice"].split(".")[1]) == 6 and "," not in fila["indice"]


def test_nunca_sobrescribe_una_prediccion_existente(tmp_path):
    ruta = escribir_csv(_df(), tmp_path, T4, "demanda_v1", AHORA)
    antes = ruta.read_bytes()

    with pytest.raises(FileExistsError, match="no se sobrescriben"):
        escribir_csv(_df(motivo="otro"), tmp_path, T4, "demanda_v1", AHORA)

    assert ruta.read_bytes() == antes
    otra = escribir_csv(_df(), tmp_path, T4, "demanda_v1", AHORA + timedelta(seconds=1))
    assert otra != ruta


def test_comas_comillas_saltos_de_linea_y_tildes_sobreviven_al_csv(tmp_path):
    motivo = 'Puente, "Barça–Madrid"\ny ñandú'
    ruta = escribir_csv(_df(motivo=motivo), tmp_path, T4, "demanda_v1", AHORA)

    filas = list(csv.DictReader(io.StringIO(ruta.read_text(encoding="utf-8"), newline="")))

    assert len(filas) == 92 and {f["motivo"] for f in filas} == {motivo}
    assert filas[0]["viajeros_previstos"].isdigit()


def test_resumen_de_coherencia_separa_laborables_fines_de_semana_festivos_y_eventos():
    cal = construir_calendario(
        T4,
        pd.DataFrame([(date(2026, 12, 25), "Navidad")], columns=["fecha", "nombre"]),
        pd.DataFrame([(date(2026, 11, 26), "Partido", "MAD")], columns=["fecha", "descripcion", "ciudad"]),
        pd.DataFrame([], columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]),
    )
    valores = []
    for fila in cal.dias.itertuples(index=False):
        if fila.festivo:
            valores.append(0.5)
        elif fila.eventos:
            valores.append(1.5)
        elif fila.dia_semana in ("sáb", "dom"):
            valores.append(2.0)
        else:
            valores.append(1.0)
    df = pd.DataFrame({"fecha": [d.isoformat() for d in DIAS], "indice": valores})

    resumen = resumen_coherencia(df, cal)

    assert resumen["laborables_sin_evento"] == pytest.approx(1.0)
    assert resumen["fines_de_semana"] == pytest.approx(2.0)
    assert resumen["festivos"] == pytest.approx(0.5)
    assert resumen["dias_con_evento"] == pytest.approx(1.5)
    assert resumen["minimo"] == 0.5 and resumen["maximo"] == 2.0 and resumen["desviacion"] > 0


def test_formatear_resumen_muestra_nd_cuando_no_hay_dias_de_un_tipo():
    texto = formatear_resumen(
        {"laborables_sin_evento": 1.0, "fines_de_semana": 1.234, "festivos": None, "dias_con_evento": None,
         "desviacion": 0.2, "minimo": 0.5, "maximo": 1.9}
    )

    assert "festivos" in texto and "n/d" in texto and "1.23" in texto and "0.20" in texto
