# Diario de desarrollo — Raillytics Light

Registro cronológico de las tareas realizadas por cada miembro del equipo. Cada entrada debe incluir fecha y nombre del desarrollador/a.

---

## 2026-07-18 — Laura

### Contexto
Arranque del trabajo de ingesta (capa Bronze). Punto de partida: repo con `README.md`, `requirements.txt`, `G3.pdf` (documento de diseño del TFM) y el esqueleto de carpetas ya creado en `feat_setup_ingesta` (commit `72ff513`).

### 1. Ramas
- No existía la rama `develop` que exige el flujo de trabajo del README (solo había `main` y `feat_setup_ingesta`, ambas en el mismo commit). Se ha creado `develop` desde `main` y se ha publicado en origin.
- Se ha creado `feat_ingesta_datos` desde `develop` para el trabajo de esta sesión (según convención `feat_*` del README).

### 2. Investigación de fuentes de datos
Repasado `G3.pdf` (documento de diseño) para confirmar las 4 fuentes previstas: Renfe Open Data, AEMET, festivos BOE, INE. Se ha comprobado el acceso real a cada una:

- **Renfe Open Data** (`data.renfe.com`, portal CKAN, API en `/api/3/action/package_show`):
  - `estaciones-listado-completo`: listado de estaciones. El recurso real se sirve desde `ssl.renfe.com`, cuyo certificado TLS está roto de forma persistente (confirmado en el propio "archiver" de data.renfe.com, que lleva fallando desde el 2025-12-17). Se descarga con verificación TLS desactivada solo para este host — es dato público, sin autenticación, y no hay alternativa con certificado válido.
  - `volumen-de-viajeros-por-franja-horaria-*`: viajeros por franja horaria y estación, con datasets por demarcación (nacional Cercanías, Madrid, Barcelona, etc.). No es serie temporal por fecha, es un perfil agregado por franja horaria.
  - `compromiso-puntualidad`: **no** es un histórico de puntualidad real, son las condiciones de indemnización por tipo de tren/retraso (dataset estático). Renfe Open Data no publica series de puntualidad real por línea y fecha — riesgo ya anticipado en el documento de diseño ("calidad irregular de Renfe Open Data").
- **AEMET OpenData** (`opendata.aemet.es`): API REST de dos pasos (la petición inicial devuelve una URL temporal con los datos). Requiere API key gratuita, que hay que solicitar por email en https://opendata.aemet.es/centrodedescargas/altaUsuario. **Pendiente**: nadie del equipo la ha solicitado todavía, así que no se ha podido descargar nada de AEMET en esta sesión.
- **Festivos BOE**: el BOE **no** ofrece los festivos como dataset estructurado (CSV/JSON). Cada año publica una Resolución de la Dirección General de Trabajo como texto legal (XML/PDF), no como tabla. Se ha optado por capturar el XML oficial de la Resolución de cada año tal cual en Bronze, dejando el parseo de la tabla de festivos para la capa Silver. Localizadas y descargadas las resoluciones de 2024, 2025 y 2026:
  - 2024 → `BOE-A-2023-22014`
  - 2025 → `BOE-A-2024-21316`
  - 2026 → `BOE-A-2025-21667`
- **INE** (API JSON Tempus3, `servicios.ine.es`): sin necesidad de API key. Descargada la tabla 2853 (población total nacional por año, histórico). **Pendiente**: localizar la tabla equivalente desglosada por provincia (la tabla 2853 solo da el total nacional) para poder cruzar con movilidad/población por provincia como describe el diseño.

### 3. Descarga de datos (capa Bronze)
Descargados los datos reales disponibles hoy en `data/bronze/<fuente>/.../fecha_descarga=2026-07-18/` (partición por fecha, tal como define la arquitectura del README):

- `bronze/renfe/estaciones/` — `estaciones.csv` (listado completo de estaciones Renfe)
- `bronze/renfe/viajeros_franja/` — `cercanias_nacional.csv`, `madrid.csv`
- `bronze/renfe/puntualidad/` — `compromiso_puntualidad.csv`
- `bronze/festivos/` — XML de las resoluciones BOE 2024, 2025 y 2026
- `bronze/ine/` — `poblacion_nacional_tabla2853.json`
- `bronze/aemet/` — sin datos todavía (falta API key, ver más arriba)

Nota: `data/` está en `.gitignore` (solo se versionan los `.gitkeep`), así que estos ficheros son locales y no se han subido al repo, tal como indica el README.

### 4. Scripts de ingesta reproducibles
Escritos en `src/raillytics/ingesta/` para poder repetir estas descargas de forma automatizada (y no solo la descarga manual de hoy):
- `renfe.py`, `festivos.py`, `ine.py`, `aemet.py` — un módulo por fuente, cada uno con su función `ingestar()`.
- `run_ingesta.py` — orquestador que lanza las cuatro.
- `utils/http.py` — descarga con idempotencia (si el fichero de la partición del día ya existe, no se vuelve a descargar).

### 5. Bloqueo detectado: entorno Python
Esta máquina **no tiene Python instalado** (solo el stub de Microsoft Store, sin intérprete real). Por eso las descargas de hoy se han hecho directamente por HTTP y los scripts anteriores están escritos pero **no se han podido ejecutar ni probar todavía**. Antes de seguir con la capa Silver hace falta:
1. Instalar Python (3.11 o 3.12) en esta máquina.
2. Crear el entorno virtual e instalar `requirements.txt`.
3. Ejecutar `python -m raillytics.ingesta.run_ingesta` y verificar que reproduce las descargas de hoy.

### Próximos pasos
- Instalar Python y validar los scripts de ingesta end-to-end.
- Solicitar la API key de AEMET y completar la ingesta de meteorología.
- Buscar la tabla INE de población por provincia (la 2853 actual es solo nacional).
- Decidir con el equipo qué hacer con la falta de un histórico real de puntualidad (Renfe Open Data no lo publica).
- Añadir tests básicos de la ingesta en `tests/`.
