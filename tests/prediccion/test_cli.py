import duckdb
import pytest

from raillytics.prediccion import __main__ as cli
from raillytics.prediccion.nivel import NivelError
from raillytics.prediccion.servicio import Resultado
from raillytics.prediccion.trimestre import Trimestre


def test_los_valores_por_defecto():
    args = cli.parsear(["--trimestre", "2026-T4"])

    assert args.trimestre == Trimestre(2026, 4)
    assert (args.prompt, args.total_esperado, args.solo_nivel, args.mostrar_prompt) == ("demanda_v1", None, False, False)


def test_todas_las_opciones():
    args = cli.parsear(
        ["--trimestre", "2027-T1", "--prompt", "demanda_v2", "--total-esperado", "4200000", "--solo-nivel", "--mostrar-prompt"]
    )

    assert args.trimestre == Trimestre(2027, 1) and args.prompt == "demanda_v2"
    assert args.total_esperado == 4_200_000 and args.solo_nivel and args.mostrar_prompt


@pytest.mark.parametrize(
    "argumentos",
    [[], ["--trimestre", "2026-T5"], ["--trimestre", "2026-Q4"], ["--trimestre", "2026-T4", "--total-esperado", "0"],
     ["--trimestre", "2026-T4", "--total-esperado", "-5"], ["--trimestre", "2026-T4", "--total-esperado", "mucho"]],
)
def test_los_argumentos_invalidos_terminan_con_error_de_uso(argumentos):
    with pytest.raises(SystemExit) as salida:
        cli.parsear(argumentos)

    assert salida.value.code == 2


@pytest.fixture
def sin_infraestructura(monkeypatch):
    monkeypatch.setattr(cli, "_conectar", lambda layout, env: duckdb.connect())


def test_main_devuelve_cero_y_pasa_los_argumentos_al_servicio(monkeypatch, sin_infraestructura):
    llamadas = []

    def falso(trimestre, **kwargs):
        llamadas.append((trimestre, kwargs))
        return Resultado(None, 1, None, None, None)

    monkeypatch.setattr(cli, "ejecutar", falso)

    codigo = cli.main(["--trimestre", "2026-T4", "--solo-nivel", "--total-esperado", "7"], env={})

    trimestre, kwargs = llamadas[0]
    assert codigo == 0 and trimestre == Trimestre(2026, 4)
    assert kwargs["solo_nivel"] is True and kwargs["total_manual"] == 7 and kwargs["cliente"] is None
    assert kwargs["version_prompt"] == "demanda_v1"


def test_main_crea_el_cliente_de_ollama_salvo_en_solo_nivel(monkeypatch, sin_infraestructura):
    clientes = []
    monkeypatch.setattr(cli, "ejecutar", lambda t, **kw: clientes.append(kw["cliente"]) or Resultado(None, 1, None, None, None))

    cli.main(["--trimestre", "2026-T4"], env={"OLLAMA_MODEL": "phi4"})

    assert clientes[0].settings.modelo == "phi4"


def test_main_traduce_los_errores_de_dominio_a_codigo_1_con_mensaje(monkeypatch, sin_infraestructura, capsys):
    def falla(trimestre, **kwargs):
        raise NivelError("faltan trimestres. Pasa --total-esperado N")

    monkeypatch.setattr(cli, "ejecutar", falla)

    codigo = cli.main(["--trimestre", "2026-T4"], env={})

    assert codigo == 1
    assert "ERROR: faltan trimestres" in capsys.readouterr().err
