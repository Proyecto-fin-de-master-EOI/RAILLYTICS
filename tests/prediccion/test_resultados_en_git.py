"""Lo que escribe `make 07_prediccion` se guarda en resultados/predicciones/, un directorio del repo que está en git."""
import shutil
import subprocess
from pathlib import Path

import pytest

from raillytics.prediccion.salida import RAIZ_POR_DEFECTO

RAIZ = Path(__file__).resolve().parents[2]
CSV = "AVE-MAD-BCN/2026-T4/demanda_diaria_2026-T4_demanda_v2_20261001T120000Z.csv"
requiere_git = pytest.mark.skipif(shutil.which("git") is None or not (RAIZ / ".git").exists(), reason="requiere un checkout de git")


def _ignorado(ruta):
    """`git check-ignore -q`: 0 = ignorado, 1 = no ignorado (cualquier otro código es un error de git)."""
    codigo = subprocess.run(["git", "check-ignore", "-q", ruta], cwd=RAIZ).returncode
    assert codigo in (0, 1), f"git check-ignore falló ({codigo})"
    return codigo == 0


@requiere_git
def test_los_csv_de_la_prediccion_no_los_ignora_git_pero_data_si_se_sigue_ignorando():
    assert not _ignorado(f"{RAIZ_POR_DEFECTO.as_posix()}/{CSV}")
    assert _ignorado(f"data/predicciones/{CSV}")  # por eso la salida ya no vive en data/ (control: el check funciona)


def test_el_directorio_existe_en_el_repo_para_que_docker_no_lo_cree_como_root():
    # Si falta en el anfitrión, Docker crea el volumen del contenedor de Airflow como root y la tarea `predecir` (uid 1000) no podría escribir.
    assert (RAIZ / RAIZ_POR_DEFECTO / ".gitkeep").is_file()


def test_el_env_example_apunta_al_mismo_directorio_que_el_codigo():
    linea = next(l for l in (RAIZ / ".env.example").read_text(encoding="utf-8").splitlines() if l.startswith("PREDICCIONES_ROOT="))

    assert Path(linea.split("=", 1)[1].strip()) == RAIZ_POR_DEFECTO
