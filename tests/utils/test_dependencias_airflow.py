"""El contenedor de Airflow tiene su propia lista de dependencias, aparte de requirements.txt.

Vive en `_PIP_ADDITIONAL_REQUIREMENTS` de docker/docker-compose.yml y hay que mantenerla a mano. Si
le falta algo que el DAG necesita, Airflow **no da un error visible**: el fichero del DAG no se puede
importar, el DAG desaparece de la lista y cualquier run que se dispare se queda en cola para siempre.
Pasó el 2026-10-08 al añadir `defusedxml` a requirements.txt y no aquí: costó media hora de carga
end-to-end agotando su tiempo de espera.

Este test recorre lo que importa el DAG de verdad —siguiendo la cadena dentro del propio repo— y
comprueba que cada paquete de terceros esté declarado en el compose.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[2]
DAG = RAIZ / "dags" / "ingesta_data_sources.py"
COMPOSE = RAIZ / "docker" / "docker-compose.yml"
PAQUETES = RAIZ / "python"

# Lo que trae de serie la imagen de Airflow: no hace falta declararlo. Comprobado el 2026-10-08
# contra apache/airflow:3.3.2-python3.12 con:
#   docker compose exec airflow-scheduler python -c "import pandas, numpy, urllib3, dotenv"
# Si se cambia de versión de imagen, vale la pena repetirlo: un paquete que deje de venir de serie
# haría desaparecer el DAG sin ningún mensaje.
YA_EN_LA_IMAGEN = {
    "airflow", "pendulum", "sqlalchemy", "pydantic", "jinja2", "dateutil",
    "pandas", "numpy", "urllib3", "dotenv",
}

# Nombre de import -> nombre del paquete que se instala, cuando no coinciden.
NOMBRE_PIP = {"yaml": "PyYAML", "dotenv": "python-dotenv", "dateutil": "python-dateutil"}


def _imports_de(fichero: Path) -> set[str]:
    """Los módulos de primer nivel que importa un fichero, sin ejecutarlo."""
    arbol = ast.parse(fichero.read_text(encoding="utf-8"))
    modulos = set()
    for nodo in ast.walk(arbol):
        if isinstance(nodo, ast.Import):
            modulos.update(a.name.split(".")[0] for a in nodo.names)
        elif isinstance(nodo, ast.ImportFrom) and nodo.module and nodo.level == 0:
            modulos.add(nodo.module.split(".")[0])
    return modulos


def _fichero_de(modulo: str) -> Path | None:
    """Dónde vive un módulo `raillytics.*` dentro del repo."""
    ruta = PAQUETES / Path(*modulo.split("."))
    for candidato in (ruta.with_suffix(".py"), ruta / "__init__.py"):
        if candidato.is_file():
            return candidato
    return None


def _cadena_de_imports(entrada: Path) -> set[str]:
    """Todo lo que acaba importándose al cargar el DAG, siguiendo los módulos del repo."""
    pendientes, vistos, externos = [entrada], set(), set()
    while pendientes:
        fichero = pendientes.pop()
        if fichero in vistos:
            continue
        vistos.add(fichero)
        for modulo in _imports_de(fichero):
            if modulo == "raillytics":
                continue
            externos.add(modulo)
        # Los `from raillytics.x.y import z` hay que seguirlos: ahí estaba el defusedxml escondido.
        arbol = ast.parse(fichero.read_text(encoding="utf-8"))
        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.ImportFrom) and nodo.module and nodo.module.startswith("raillytics"):
                siguiente = _fichero_de(nodo.module)
                if siguiente:
                    pendientes.append(siguiente)
    return externos


def _declarados_en_el_compose() -> set[str]:
    texto = COMPOSE.read_text(encoding="utf-8")
    casa = re.search(r'_PIP_ADDITIONAL_REQUIREMENTS:\s*"([^"]*)"', texto)
    assert casa, "no se encuentra _PIP_ADDITIONAL_REQUIREMENTS en docker-compose.yml"
    # «requests~=2.32» -> «requests»
    return {re.split(r"[~=<>\[]", p)[0].strip().lower() for p in casa.group(1).split() if p.strip()}


def test_el_dag_no_importa_nada_que_falte_en_el_contenedor_de_airflow():
    externos = _cadena_de_imports(DAG)
    declarados = _declarados_en_el_compose()
    estandar = sys.stdlib_module_names

    faltan = sorted(
        NOMBRE_PIP.get(m, m)
        for m in externos
        if m not in estandar and m not in YA_EN_LA_IMAGEN
        and NOMBRE_PIP.get(m, m).lower() not in declarados
    )

    assert not faltan, (
        f"el DAG importa {', '.join(faltan)}, que no está en _PIP_ADDITIONAL_REQUIREMENTS de "
        f"docker/docker-compose.yml. Sin eso el DAG no se puede importar y Airflow lo descarta "
        f"EN SILENCIO: desaparece de la lista y los runs se quedan en cola."
    )


def test_defusedxml_sigue_declarado():
    """Caso concreto del fallo que originó este test, por si alguien limpia la lista."""
    assert "defusedxml" in _declarados_en_el_compose()


@pytest.mark.parametrize("fichero", [DAG, COMPOSE])
def test_los_ficheros_que_vigila_este_test_existen(fichero):
    """Si alguien mueve el DAG o el compose, que falle aquí y no en una carga de media hora."""
    assert fichero.is_file(), f"no existe {fichero}"
