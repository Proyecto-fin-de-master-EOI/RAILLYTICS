from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from raillytics.prediccion.calendario import construir_calendario
from raillytics.prediccion.gates import dia_semana_del_motivo
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.reglas import ReglasError, cargar_reglas, combinar, es_modo_eventos, indices_base
from raillytics.prediccion.trimestre import Trimestre

RAIZ = Path(__file__).resolve().parents[2]
T4 = Trimestre(2026, 4)
REGLAS = cargar_reglas(RAIZ / "config" / "reglas_demanda.yml")
FESTIVOS_2026_T4 = [
    (date(2026, 10, 12), "Fiesta Nacional de España"),  # lunes
    (date(2026, 11, 1), "Todos los Santos"),  # domingo
    (date(2026, 12, 6), "Día de la Constitución"),  # domingo
    (date(2026, 12, 8), "Inmaculada Concepción"),  # martes: el lunes 7 es puente
    (date(2026, 12, 25), "Navidad"),  # viernes
]


def _calendario(*eventos):
    return construir_calendario(
        T4,
        pd.DataFrame(FESTIVOS_2026_T4, columns=["fecha", "nombre"]),
        pd.DataFrame(list(eventos), columns=["fecha", "descripcion", "ciudad"]),
        pd.DataFrame([], columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]),
    )


def test_el_fichero_de_reglas_del_repo_se_carga():
    assert REGLAS.base_dia_semana == {"lun": 1.10, "mar": 1.00, "mié": 1.00, "jue": 1.10, "vie": 1.30, "sáb": 0.85, "dom": 1.25}
    assert (REGLAS.festivo_entre_semana, REGLAS.puente) == (0.70, 0.75)
    assert (REGLAS.vispera, REGLAS.regreso, REGLAS.junto_a_evento) == (0.20, 0.20, 0.05)
    assert REGLAS.como_dict()["base_dia_semana"]["vie"] == 1.30


def test_un_fichero_de_reglas_incompleto_o_ausente_falla_con_un_mensaje_claro(tmp_path):
    incompleto = tmp_path / "reglas.yml"
    incompleto.write_text("base_dia_semana: {lun: 1.1}\n", encoding="utf-8")

    with pytest.raises(ReglasError, match="festivo_entre_semana, puente y ajustes"):
        cargar_reglas(incompleto)
    with pytest.raises(ReglasError, match="no existe"):
        cargar_reglas(tmp_path / "otro.yml")


@pytest.mark.parametrize(
    ("dia", "valor", "motivo"),
    [
        (date(2026, 10, 6), 1.00, "martes laborable"),
        (date(2026, 10, 9), 1.50, "viernes víspera de tramo festivo"),  # 1.30 + 0.20
        (date(2026, 10, 10), 0.85, "sábado de fin de semana"),  # dentro del tramo, pero ni víspera ni regreso
        (date(2026, 10, 12), 0.90, "lunes festivo (Fiesta Nacional de España), regreso de tramo festivo"),  # 0.70 + 0.20
        (date(2026, 11, 1), 1.45, "domingo festivo (Todos los Santos), regreso de tramo festivo"),  # en domingo: 1.25 + 0.20
        (date(2026, 12, 6), 1.25, "domingo festivo (Día de la Constitución)"),
        (date(2026, 12, 7), 0.75, "lunes puente"),
        (date(2026, 12, 25), 0.70, "viernes festivo (Navidad)"),
        (date(2026, 12, 27), 1.45, "domingo regreso de tramo festivo"),  # 1.25 + 0.20
    ],
)
def test_indices_base_con_los_festivos_nacionales_de_2026_T4(dia, valor, motivo):
    base = {i.fecha: i for i in indices_base(_calendario(), REGLAS)}

    assert base[dia].indice == pytest.approx(valor) and base[dia].motivo == motivo


def test_cada_motivo_de_las_reglas_empieza_por_su_dia_de_la_semana():
    base = indices_base(_calendario(), REGLAS)

    assert len(base) == 92 and all(dia_semana_del_motivo(i.motivo) == i.fecha.weekday() for i in base)


def test_combinar_aplica_el_factor_del_evento_y_el_ajuste_de_los_dias_de_al_lado():
    cal = _calendario((date(2026, 11, 18), "Partido", "MAD"))  # miércoles
    factores = [IndiceDia(date(2026, 11, 18), 1.20, "miércoles con partido en Madrid")]

    final = {i.fecha: i for i in combinar(indices_base(cal, REGLAS), factores, cal, REGLAS)}

    assert final[date(2026, 11, 18)].indice == pytest.approx(1.20)  # 1.00 × 1.20
    assert final[date(2026, 11, 18)].motivo == "miércoles con partido en Madrid · miércoles laborable"  # el del LLM, primero
    assert final[date(2026, 11, 17)].indice == pytest.approx(1.05)  # martes: 1.00 + 0.05
    assert final[date(2026, 11, 19)].indice == pytest.approx(1.15)  # jueves: 1.10 + 0.05
    assert final[date(2026, 11, 19)].motivo == "jueves laborable, junto a un evento"
    assert final[date(2026, 11, 20)].indice == pytest.approx(1.30)  # el viernes no está junto al evento


def test_un_evento_que_el_llm_valora_sin_efecto_no_sube_los_dias_de_al_lado():
    cal = _calendario((date(2026, 11, 18), "Congreso local", "BCN"))
    factores = [IndiceDia(date(2026, 11, 18), 1.00, "miércoles con congreso local")]

    final = {i.fecha: i for i in combinar(indices_base(cal, REGLAS), factores, cal, REGLAS)}

    assert final[date(2026, 11, 17)].indice == pytest.approx(1.00) and "junto" not in final[date(2026, 11, 17)].motivo


def test_combinar_exige_el_factor_de_cada_dia_con_evento():
    cal = _calendario((date(2026, 11, 18), "Partido", "MAD"))

    with pytest.raises(ValueError, match="2026-11-18"):
        combinar(indices_base(cal, REGLAS), [], cal, REGLAS)


def test_el_modo_eventos_se_elige_por_el_prefijo_de_la_plantilla():
    assert es_modo_eventos("eventos_v1") and not es_modo_eventos("demanda_v4")
