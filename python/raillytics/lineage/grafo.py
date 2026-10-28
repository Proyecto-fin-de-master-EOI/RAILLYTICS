"""Grafo de lineage DECLARADO: qué fuentes, tablas, reglas y dashboards existen y cómo se conectan, leído del repositorio.

Lo observado (qué se cargó, cuándo, con qué resultado) vive en el lake (`_trazabilidad/cargas` y `/calidad`); este grafo es lo que
debería existir. Los dos se cruzan en los datasets de Superset (raillytics.lineage.datasets), de modo que un nodo declarado que nunca se
ha ejecutado se ve como «sin ejecutar» en vez de no aparecer.

Fuentes de cada parte del grafo:
  fuente → staging → L1 → L2   config/data_sources.yml (cada fuente recorre las tres apps de ingesta)
  L2 → Silver                  la clave `silver:` de cada fuente + src/main/resources/silver/<tabla>.sql
  Silver/Gold → Gold           las tablas `silver_*`/`gold_*` que cita cada src/main/resources/gold/<tabla>.sql (la trazabilidad solo
                               guarda «origen = s3a://raillytics-silver», sin decir de qué tablas)
  generadores sintéticos       silver_sample y prediccion.muestra (constantes de sus módulos)
  tablas que construye Python  DERIVADAS: festivos, meteo, oferta del NAP, tabla de entrenamiento, KPI del modelo y del precio…
                               (no pasan por Spark ni declaran nada que leer, así que sus entradas se declaran aquí)
  predicción                  config/prediccion.yml (de qué lee) y las constantes de salida de raillytics.prediccion
  Gold → dataset → dashboard   dashboards/superset/: el `read_parquet('s3://raillytics-gold/<tabla>/')` de cada dataset y los gráficos
                               colocados en cada dashboard
  reglas de calidad            config/quality_gates.yml
"""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from raillytics.ingesta.sources import load_sources
from raillytics.prediccion.gates import TABLA as TABLA_PREDICCION
from raillytics.prediccion.muestra import FUENTES as FUENTES_MUESTRA
from raillytics.prediccion.publicacion import GOLD_TABLA as GOLD_PREDICCION
from raillytics.procesamiento.silver_sample import SILVER_PUNTUALIDAD, SILVER_VIAJEROS

# capa -> (orden de izquierda a derecha, etiqueta). El id de un nodo es «<capa>:<nombre>».
CAPAS: dict[str, tuple[int, str]] = {
    "fuente": (0, "Fuente"),
    "gen": (0, "Generador sintético"),
    "modelo": (0, "Modelo LLM"),
    "staging": (1, "Staging"),
    "l1": (2, "Bronze L1"),
    "l2": (3, "Bronze L2"),
    "silver": (4, "Silver"),
    "pred": (5, "Predicción"),
    "gold": (6, "Gold"),
    "dataset": (7, "Dataset de Superset"),
    "dashboard": (8, "Dashboard"),
}

# Los nombres de proceso son los que cada app registra en la trazabilidad (columna `proceso` de `cargas`).
DESCARGA, L1, L2 = "bronze_download", "bronze_l1_raw_uploader", "bronze_l2_parquet_converter"
SILVER, SILVER_SAMPLE, GOLD = "silver_builder", "silver_sample", "gold_build"
PREDICCION, MUESTRA, SUPERSET = "prediccion_demanda", "prediccion_muestra", "superset"

RUTA_SILVER = "src/main/resources/silver"
RUTA_GOLD = "src/main/resources/gold"
BASE_DASHBOARDS = Path("dashboards/superset/raillytics_gold")
MODELO = "ollama"


@dataclass(frozen=True)
class Nodo:
    id: str
    capa: str
    nombre: str
    descripcion: str = ""
    ruta: str = ""          # fichero que lo define (SQL, YAML) o URL de la fuente


@dataclass(frozen=True)
class Arista:
    origen: str
    destino: str
    proceso: str
    transformacion: str     # qué hace el salto, en una frase
    ruta: str = ""          # el fichero de la transformación, si lo hay (SQL)


@dataclass(frozen=True)
class Regla:
    nodo: str
    gate: str
    tipo: str
    severidad: str
    descripcion: str


@dataclass
class Grafo:
    nodos: list[Nodo] = field(default_factory=list)
    aristas: list[Arista] = field(default_factory=list)
    reglas: list[Regla] = field(default_factory=list)


@dataclass(frozen=True)
class Derivada:
    proceso: str            # el que registra la carga (primer argumento de registrar_carga)
    salida: str             # id del nodo que escribe
    entradas: tuple[str, ...]   # ids de nodo; `fuente:<patrón>` son todas las fuentes del registro que encajan (fnmatch)
    descripcion: str
    modulo: str             # fichero que la construye


# Tablas que construye Python y no Spark. Las de la ingesta descargan su fuente ellas mismas (no pasan por L1/L2), así que la arista sale
# de la fuente. Los nombres van como texto para no importar scikit-learn al generar el grafo; un test los contrasta con las constantes
# de cada módulo. fact_cobertura_ingesta no tiene entradas: lee el registro de cargas, que es metadato del lake y no un nodo.
DERIVADAS: tuple[Derivada, ...] = (
    Derivada("referencia_festivos", "silver:festivos", ("fuente:boe_calendario_laboral_*",),
             "Festivos de Madrid o Cataluña del calendario laboral del BOE", "python/raillytics/ingesta/boe_festivos.py"),
    Derivada("referencia_meteo", "silver:meteo", ("fuente:aemet_*",),
             "Temperatura media y precipitación diarias de AEMET en las dos cabeceras del corredor",
             "python/raillytics/ingesta/aemet_historico.py"),
    Derivada("referencia_turismo_mensual", "silver:turismo_mensual", ("fuente:ine_turismo*",),
             "Turistas mensuales entre Madrid y Barcelona del INE", "python/raillytics/ingesta/ine_turismo.py"),
    Derivada("referencia_oferta_diaria", "silver:oferta_diaria", ("fuente:nap_gtfs_*",),
             "Trenes por día, corredor y operador del histórico GTFS del NAP", "python/raillytics/ingesta/nap_oferta.py"),
    Derivada("referencia_oferta_franja", "silver:oferta_franja", ("fuente:nap_gtfs_*",),
             "Trenes por día, corredor y franja horaria del histórico GTFS del NAP", "python/raillytics/ingesta/nap_oferta.py"),
    Derivada("referencia_ml_ocupacion_diaria", "silver:ml_ocupacion_diaria",
             ("silver:oferta_diaria", "silver:cnmc_trimestral", "silver:festivos", "silver:meteo"),
             "Tabla de entrenamiento: ocupación diaria implicada con sus variables de calendario y meteo", "python/raillytics/ml/dataset.py"),
    Derivada("ml_metricas", "gold:fact_metricas_modelo", ("silver:ml_ocupacion_diaria",),
             "Métricas del modelo de ocupación diaria (MAPE, R², intervalo)", "python/raillytics/ml/modelo.py"),
    Derivada("ml_ocupacion_franja", "gold:fact_ocupacion_franja", ("silver:oferta_franja", "silver:ml_ocupacion_diaria"),
             "Ocupación estimada por día, operador y franja horaria", "python/raillytics/ml/franja.py"),
    Derivada("precio_recomendado", "gold:fact_precio_recomendado", ("gold:fact_ocupacion_franja", "silver:cnmc_precio_trimestral"),
             "Precio recomendado por día, franja y operador", "python/raillytics/prediccion/precio.py"),
    Derivada("calidad_completitud_silver", "gold:fact_completitud_silver",
             ("silver:viajeros_enriquecidos", "silver:puntualidad_enriquecida", "silver:cnmc_trimestral",
              "silver:cnmc_precio_trimestral", "silver:cnmc_precio_mensual", "silver:oferta_diaria", "silver:festivos",
              "silver:meteo", "silver:turismo_mensual", "silver:ml_ocupacion_diaria"),
             "KPI de completitud de las tablas de Silver", "python/raillytics/calidad/completitud_silver.py"),
    Derivada("calidad_cobertura_ingesta", "gold:fact_cobertura_ingesta", (),
             "KPI de cobertura de la ingesta, calculado del registro de cargas", "python/raillytics/calidad/cobertura_ingesta.py"),
)


# ------------------------------------------------------------------------------------------------ utilidades de SQL

def sin_comentarios(sql: str) -> str:
    """El SQL sin comentarios de línea (`-- …`) ni de bloque (`/* … */`): un nombre citado en un comentario no es una dependencia."""
    return re.sub(r"--[^\n]*", "", re.sub(r"/\*.*?\*/", "", sql, flags=re.S))


def tabla_que_escribe(sql: str, por_defecto: str) -> str:
    """La tabla Gold que construye el fichero, que no siempre se llama como él.

    Por convención el SQL se llama como su tabla y no hace falta decir nada. La excepción son las
    versiones ALTERNATIVAS: `dim_fecha_corredor.sql` escribe `dim_fecha` (GoldBuilder elige una u
    otra según haya grupo «corredor»; ver SqlAlternativo en GoldBuilder.scala). Sin esto el grafo
    creaba un nodo por FICHERO y el alternativo salía siempre como «sin ejecutar», porque la
    trazabilidad registra la carga con el nombre de la TABLA y ese nodo no existía.

    Se declara en el propio SQL con una línea `-- tabla: <nombre>`, para que el fichero diga qué
    construye sin mantener la correspondencia en un segundo sitio.
    """
    encontrado = re.search(r"^--\s*tabla:\s*([a-z][a-z0-9_]*)\s*$", sql, re.I | re.M)
    return encontrado.group(1) if encontrado else por_defecto


def descripcion_de_sql(sql: str) -> str:
    """El primer comentario de línea del fichero, sin el guion: por convención dice qué construye la consulta."""
    for linea in sql.splitlines():
        if linea.startswith("--"):
            return linea[2:].strip()
        if linea.strip():
            break
    return ""


def _describir_regla(gate: dict) -> str:
    tipo = gate["tipo"]
    if tipo == "filas_min":
        return f"al menos {gate['minimo']} filas"
    if tipo == "no_nulos":
        return "sin nulos en " + ", ".join(gate["columnas"])
    if tipo == "unico":
        return "sin duplicados por " + ", ".join(gate["columnas"])
    if tipo == "dominio":
        return f"{gate['columna']} dentro de {', '.join(str(v) for v in gate['valores'])}"
    if tipo == "rango":
        limites = [f"≥ {gate['minimo']}" if "minimo" in gate else "", f"≤ {gate['maximo']}" if "maximo" in gate else ""]
        return f"{gate['columna']} " + " y ".join(x for x in limites if x)
    if tipo == "referencia":
        return "{} existen en {}".format(", ".join(gate["columnas"]), gate["tabla"])
    return "consulta SQL · " + gate["nombre"].replace("_", " ")


# ------------------------------------------------------------------------------------------------ construcción

class _Constructor:
    def __init__(self, raiz: Path) -> None:
        self.raiz = raiz
        self.nodos: dict[str, Nodo] = {}
        self.aristas: dict[tuple[str, str, str], Arista] = {}

    def nodo(self, capa: str, nombre: str, descripcion: str = "", ruta: str = "") -> str:
        id_ = f"{capa}:{nombre}"
        previo = self.nodos.get(id_)
        # Una descripción o ruta que llega después completa al nodo creado «bajo demanda» por una arista.
        if previo is None or (descripcion and not previo.descripcion) or (ruta and not previo.ruta):
            self.nodos[id_] = Nodo(id_, capa, nombre, descripcion or (previo.descripcion if previo else ""),
                                   ruta or (previo.ruta if previo else ""))
        return id_

    def arista(self, origen: str, destino: str, proceso: str, transformacion: str, ruta: str = "") -> None:
        self.aristas[(origen, destino, proceso)] = Arista(origen, destino, proceso, transformacion, ruta)

    def sql(self, relativa: str) -> tuple[str, str]:
        """(texto del SQL, ruta relativa) de un recurso; vacío si no existe."""
        ruta = self.raiz / relativa
        return (ruta.read_text(encoding="utf-8"), relativa) if ruta.is_file() else ("", "")

    # -- ingesta y Silver
    def fuentes(self) -> None:
        origen_config = self.raiz / "config" / "data_sources.yml"
        if not origen_config.is_file():
            return
        for fuente in load_sources(origen_config):
            f = self.nodo("fuente", fuente.id, fuente.name, fuente.url)
            s = self.nodo("staging", fuente.id, "Fichero descargado, a la espera de L1")
            l1 = self.nodo("l1", fuente.id, "Copia literal en MinIO (l1-raw)")
            l2 = self.nodo("l2", fuente.id, "Parquet en MinIO (l2)")
            self.arista(f, s, DESCARGA, "Descarga HTTP del DAG ingesta_data_sources (Airflow) con quality gates de fichero")
            self.arista(s, l1, L1, "Copia el fichero tal cual a MinIO (Spark Streaming · RawUploader)")
            self.arista(l1, l2, L2, "CSV, JSON o ZIP a Parquet (Spark Streaming · ParquetConverter)")
            for tabla in fuente.silver:
                texto, ruta = self.sql(f"{RUTA_SILVER}/{tabla.tabla}.sql")
                silver = self.nodo("silver", tabla.tabla, descripcion_de_sql(texto), ruta)
                self.arista(l2, silver, SILVER,
                            f"silver/{tabla.tabla}.sql ({tabla.modo}) · Spark Structured Streaming · SilverBuilder", ruta)

    def generadores(self) -> None:
        silver_sample = self.nodo("gen", "silver_sample", "Silver sintético y determinista del corredor (no son datos reales)")
        for tabla in (SILVER_VIAJEROS, SILVER_PUNTUALIDAD):
            silver = self.nodo("silver", tabla)
            self.arista(silver_sample, silver, SILVER_SAMPLE,
                        "Generador determinista (python -m raillytics.procesamiento.silver_sample)")
        # La predicción ya lee datos reales (config/prediccion.yml): la muestra queda para probarla sin ellos y no llega más allá de L2.
        muestra = self.nodo("gen", "muestra_prediccion", "Festivos, eventos, meteo y demanda sintéticos para probar la predicción sin datos reales")
        for fuente in FUENTES_MUESTRA:
            l2 = self.nodo("l2", f"muestra_{fuente}", "Muestra sintética en Bronze L2")
            self.arista(muestra, l2, MUESTRA, "Generador determinista (python -m raillytics.prediccion.muestra)")

    def derivadas(self) -> None:
        fuentes = [n.nombre for n in self.nodos.values() if n.capa == "fuente"]
        for d in DERIVADAS:
            capa, _, nombre = d.salida.partition(":")
            salida = self.nodo(capa, nombre, d.descripcion, d.modulo)
            modulo = d.modulo.removeprefix("python/").removesuffix(".py").replace("/", ".")
            for entrada in d.entradas:
                capa_e, _, patron = entrada.partition(":")
                for nombre_e in fnmatch.filter(fuentes, patron) if capa_e == "fuente" else [patron]:
                    self.arista(self.nodo(capa_e, nombre_e), salida, d.proceso, f"{d.descripcion} (python -m {modulo})", d.modulo)

    # -- Gold
    def gold(self) -> None:
        carpeta = self.raiz / RUTA_GOLD
        for fichero in sorted(carpeta.glob("*.sql")) if carpeta.is_dir() else []:
            texto = fichero.read_text(encoding="utf-8")
            ruta = f"{RUTA_GOLD}/{fichero.name}"
            escribe = tabla_que_escribe(texto, fichero.stem)
            destino = self.nodo("gold", escribe, descripcion_de_sql(texto), ruta)
            for capa, tabla in sorted(set(re.findall(r"\b(silver|gold)_([a-z][a-z0-9_]*)", sin_comentarios(texto)))):
                if (capa, tabla) != ("gold", escribe):
                    self.arista(self.nodo(capa, tabla), destino, GOLD, f"gold/{fichero.name} · Spark batch · GoldBuilder", ruta)

    # -- predicción
    def prediccion(self) -> None:
        salida = self.nodo("pred", TABLA_PREDICCION, "Predicción diaria de demanda del corredor (CSV versionado)")
        gold = self.nodo("gold", GOLD_PREDICCION, "Predicción publicada en Gold para el dashboard")
        self.arista(self.nodo("modelo", MODELO, "LLM local (Ollama) que reparte el total del trimestre entre los días"),
                    salida, PREDICCION, "El LLM reparte el total esperado entre los días (raillytics.prediccion)")
        self.arista(salida, gold, PREDICCION, "Publicación en Gold de la predicción (raillytics.prediccion.publicacion)")
        config = self.raiz / "config" / "prediccion.yml"
        origenes = (yaml.safe_load(config.read_text(encoding="utf-8")) or {}).get("origenes", {}) if config.is_file() else {}
        for nombre, entrada in origenes.items():
            sql = (entrada or {}).get("sql", "")
            for tabla in re.findall(r"\{silver\}/(\w+)/", sql):
                self.arista(self.nodo("silver", tabla), salida, PREDICCION, f"Origen «{nombre}» de config/prediccion.yml")
            for fuente in re.findall(r"\{bronze\}/l2/(\w+)/", sql):
                self.arista(self.nodo("l2", fuente), salida, PREDICCION, f"Origen «{nombre}» de config/prediccion.yml")

    # -- Superset
    def superset(self) -> None:
        base = self.raiz / BASE_DASHBOARDS
        if not base.is_dir():
            return
        datasets = [yaml.safe_load(p.read_text(encoding="utf-8")) for p in sorted((base / "datasets").glob("*/*.yaml"))]
        graficos = {c["uuid"]: c for c in (yaml.safe_load(p.read_text(encoding="utf-8")) for p in sorted((base / "charts").glob("*.yaml")))}
        # Solo datasets de NEGOCIO: los que leen tablas de Gold. Los que leen `_trazabilidad` (cargas, calidad, frescura y los propios de
        # lineage) son meta-datos del lake y se excluyen por eso, no por su nombre: el SQL de lineage lleva incrustadas descripciones
        # de otros datasets con rutas de Gold y, sin esta guarda, el grafo se incluiría a sí mismo en cada regeneración.
        con_gold: dict[str, str] = {}
        for d in datasets:
            sql = d.get("sql", "")
            tablas = sorted(set(re.findall(r"s3://raillytics-gold/([a-z][a-z0-9_]*)/", sql)))
            if not tablas or "_trazabilidad" in sql:
                continue
            nodo = self.nodo("dataset", d["table_name"], (d.get("description") or "").strip().replace("\n", " "))
            con_gold[d["uuid"]] = nodo
            for tabla in tablas:
                self.arista(self.nodo("gold", tabla), nodo, SUPERSET, "SQL del dataset (DuckDB sobre el Parquet de Gold)")
        for p in sorted((base / "dashboards").glob("*.yaml")):
            panel = yaml.safe_load(p.read_text(encoding="utf-8"))
            usados = {con_gold[graficos[c["meta"]["uuid"]]["dataset_uuid"]]
                      for c in panel["position"].values()
                      if isinstance(c, dict) and c.get("type") == "CHART" and c["meta"]["uuid"] in graficos
                      and graficos[c["meta"]["uuid"]]["dataset_uuid"] in con_gold}
            if not usados:
                continue
            dashboard = self.nodo("dashboard", panel["slug"], panel["dashboard_title"])
            for dataset in sorted(usados):
                self.arista(dataset, dashboard, SUPERSET, "Gráficos del dashboard sobre el dataset")

    # -- reglas de calidad
    def reglas(self) -> list[Regla]:
        ruta = self.raiz / "config" / "quality_gates.yml"
        if not ruta.is_file():
            return []
        reglas = []
        for tabla, gates in ((yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}).get("tablas") or {}).items():
            capa, _, nombre = tabla.partition("_")
            if capa not in ("silver", "gold"):
                continue
            nodo = self.nodo(capa, nombre)
            reglas += [Regla(nodo, g["nombre"], g["tipo"], g["severidad"], _describir_regla(g)) for g in gates]
        return reglas


def construir(raiz: Path) -> Grafo:
    c = _Constructor(raiz)
    c.fuentes()
    c.generadores()
    c.derivadas()
    c.gold()
    c.prediccion()
    c.superset()
    reglas = c.reglas()
    orden = lambda n: (CAPAS[n.capa][0], n.id)   # noqa: E731
    return Grafo(
        sorted(c.nodos.values(), key=orden),
        sorted(c.aristas.values(), key=lambda a: (a.origen, a.destino, a.proceso)),
        sorted(reglas, key=lambda r: (r.nodo, r.gate)),
    )
