# Diario de desarrollo — Raillytics Light

Se crea el fichero DIARIO,md para ir apuntando, en plan diario personal, qué hacemos cada día y por qué. La idea es que cualquiera del equipo pueda leer esto y entender qué se ha hecho sin tener que preguntar. Cada entrada lleva la fecha y el nombre de quien la escribe.

---

## 2026-07-18 — Laura

Arranque del proyecto: montar la rama de trabajo y empezar a traer los datos con los que vamos a jugar (Renfe, AEMET, festivos y INE). Cuento aquí todo lo que he ido encontrando, incluidos los líos, para que si a otro le pasa lo mismo no tenga que descubrirlo de cero.

### Contexto de partida

Partiendo de la organización que Mario nos ha dejado en el repositoriol (el esqueleto), se crean las ramas como `develop` y se descargan los datos con los que vamos a trabajar.

### 1. Las ramas de Git

El README dice que todo el trabajo tiene que partir de una rama `develop`, pero resulta que **esa rama no existía todavía** — solo estaban `main` y `feat_setup_ingesta`, y encima apuntaban al mismo sitio. Así que:

1. He creado `develop` a partir de `main` y la he subido a GitHub. A partir de ahora, esta es la rama "base" de la que debe salir todo el trabajo nuevo del equipo.
2. Desde `develop` he creado `feat_ingesta_datos`, que es donde he hecho el trabajo de hoy (siguiendo la norma del README de que las ramas de funcionalidad empiezan por `feat_`).

### 2. Investigar de dónde sacamos los datos

En el documento de diseño (`G3.pdf`) habiamos indicado 4 fuentes: **Renfe Open Data, AEMET, festivos del BOE e INE**. Sobre el papel suena sencillo, pero cuando te pones a mirarlas una por una, cada una tiene su truco. Voy por partes:

**Renfe Open Data** (el portal es `data.renfe.com`)
- Tienen un listado de estaciones que se puede descargar sin problema, pero el fichero en realidad vive en otro dominio (`ssl.renfe.com`) que tiene el certificado de seguridad roto desde hace meses — no es cosa nuestra, hasta la propia Renfe lo tiene marcado como caído en su portal. Para poder descargarlo he tenido que decirle al script "no verifiques el certificado" solo para esa descarga en concreto. No es lo ideal, pero como es un dato público (no hace falta contraseña ni nada) y no hay otra forma de conseguirlo, de momento vale.
- También hay datos de "viajeros por franja horaria" (cuánta gente sube y baja en cada estación por tramos de media hora). Ojo: esto **no es un histórico día a día**, es más bien "así suele ser un día cualquiera", así que hay que tener cuidado de no confundirlo con una serie temporal real.
- Y aquí viene la sorpresa gorda: el dataset que se llama "compromiso puntualidad" **no tiene nada que ver con la puntualidad real de los trenes**. Es solo la tabla de cuánto te indemnizan si tu tren llega tarde, no un histórico de retrasos. Es decir, Renfe Open Data no publica en abierto un histórico real de puntualidad por línea y fecha. Esto ya lo avisaba el propio documento de diseño como riesgo ("calidad irregular de Renfe Open Data").

**AEMET** (la web de meteorología, `opendata.aemet.es`)
- Para pedirles datos hace falta una clave de acceso (API key) gratuita, pero hay que solicitarla por email y esperar a que te la manden. **Todavía no la tenemos**, así que hoy no he podido bajar ni un solo dato de meteorología. Queda pendiente que alguien del equipo la pida en: https://opendata.aemet.es/centrodedescargas/altaUsuario

**Festivos (BOE)**
- Aquí me llevé otra sorpresa: el BOE no tiene un Excel ni un JSON con los festivos, año a año publican un documento legal (una Resolución) explicando qué días son festivos en cada sitio, y ya está. No es una tabla, es papeleo. Así que lo que he hecho es guardarme tal cual el documento oficial de cada año (en formato XML) y dejar para más adelante (la capa "Silver", donde limpiamos los datos) el trabajo de sacar de ahí la tabla de festivos de verdad. He conseguido los documentos de 2024, 2025 y 2026.

**INE** (estadísticas oficiales)
- Esta ha sido la más fácil: tienen una API que no pide ni clave ni registro. He bajado la población total de España por año (histórico). Eso sí, lo que he conseguido es el dato **nacional**, y para lo que necesitamos (cruzar con provincias) hace falta encontrar la tabla que lo desglosa por provincia — lo dejo pendiente.

### 3. Lo que hemos descargado hoy de verdad

Todo esto está guardado en el ordenador, dentro de `data/bronze/`, organizado por fuente y por fecha de descarga (la carpeta de hoy es `fecha_descarga=2026-07-18`):

- Estaciones de Renfe
- Viajeros por franja horaria (Cercanías a nivel nacional y de Madrid)
- La tabla de indemnizaciones de Renfe (que no es puntualidad, como decíamos arriba)
- Los documentos BOE de festivos de 2024, 2025 y 2026
- La población nacional histórica del INE
- De AEMET, nada todavía (falta la clave)

Un detalle importante: la carpeta `data/` **no se sube a GitHub** (está en el `.gitignore`), así que estos ficheros se quedan solo en nuestro ordenador. La razón es porque los datos pueden pesar mucho y cambian constantemente, así que lo que sí se sube al repo son los *scripts* que los descargan, no los datos en sí.

### 4. Scripts para no tener que hacer esto a mano cada vez

Dejo los scripts en Python, dentro de `src/raillytics/ingesta/`, para descarar los datos, uno por cada fuente (`renfe.py`, `festivos.py`, `ine.py`, `aemet.py`) más uno que los lanza todos seguidos (`run_ingesta.py`). Si se vuelve a ejecutar el mismo día no descargar otra vez lo que ya tiene.


### Lo que queda pendiente

- Pedir la clave de AEMET para poder traer la meteorología.
- Buscar la tabla del INE que da población por provincia, no solo el total nacional.
- Revisar que hacemos con el tema de la puntualidad: Renfe no publica un histórico real, así que hay que decidir un plan B (¿otra fuente? ¿lo dejamos fuera del alcance?).
- Añadir algún test sencillo para la ingesta en la carpeta `tests/`.

---

## 2026-07-19 — Laura

Hoy tocaba probar de verdad lo que dejé montado ayer: ejecutar los 4 scripts contra las fuentes reales (no solo revisar el código) y comprobar que lo que traemos cubre desde 2024, que es lo que necesitamos para el modelo predictivo. De paso, repaso si lo que tenemos encaja con lo que dice el documento de diseño (`G3.pdf`) y con el README, y dejo aquí el resultado.

### 1. Ejecución de los 4 scripts (`run_ingesta.py`)

Los lancé todos seguidos contra las fuentes reales:

- **Renfe** ✅ — estaciones, viajeros por franja (Cercanías nacional y Madrid) e indemnizaciones se descargan sin problema. Son fotos del estado actual del portal, no series por fecha, así que aquí "desde 2024" no aplica tal cual.
- **Festivos (BOE)** ✅ — descarga las 3 resoluciones (2024, 2025, 2026) y comprobé el contenido del XML de 2024: es efectivamente la Resolución "para el año 2024". Cubre bien desde 2024.
- **INE** ⚠️→✅ — aquí sí había un problema de verdad: la tabla que usábamos (2853, población nacional) está **descontinuada por el propio INE desde 2021** (lo confirmé consultando la API en directo, no solo mirando lo que teníamos descargado). A partir de esa fecha el padrón anual se sustituyó por la Estadística Continua de Población (ECP), de periodicidad trimestral. Encontré la serie equivalente dentro de la ECP (`ECP4961`, "Total Nacional... Población") y cambié `ine.py` para usar esa serie en vez de la tabla vieja. Ahora sí llega hasta el segundo trimestre de 2026 — probado y confirmado con datos reales.
- **AEMET** ⚠️ — sigue sin poder probarse: no hay `AEMET_API_KEY` (no existe `.env` todavía). Y aunque la tengamos, ahora mismo el script solo pide los últimos 15 días por defecto, no hay backfill desde 2024 (la API limita cada petición a ~15 días, habría que encadenar peticiones).

Detalle técnico menor: no hay `pyproject.toml` ni instalación editable del paquete, así que para ejecutar los scripts hay que fijar `PYTHONPATH=src` (si no, da `ModuleNotFoundError: raillytics`). No lo he tocado, solo lo dejo anotado por si a alguien le pasa.

### 2. Repaso de que cumplimos lo que dice `G3.pdf` / README / `requirements.txt`

En general vamos bien encaminados, pero hay un par de cosas que el documento de diseño pide y que todavía no hacemos:

**Lo que sí cumplimos:**
- Las 4 fuentes son exactamente las que dice el G3.pdf (Renfe Open Data, AEMET, festivos BOE, INE).
- Ingesta en Python ✅, guardando el dato "tal cual, sin transformar" en Bronze ✅.
- Control de idempotencia (si ya existe el fichero del día, no se vuelve a descargar) — el G3.pdf lo pide explícitamente para la capa Bronze y lo tenemos.
- La estructura de carpetas (`/bronze/renfe`, `/bronze/aemet`, `/bronze/festivos`, `/bronze/ine`) coincide con el diagrama del README.
- Las dependencias que usan los scripts de ingesta (`requests`, `python-dotenv`) están en `requirements.txt` con las versiones correctas.

**Lo que NO cumplimos todavía (gaps reales, no hay que asustarse pero hay que apuntarlo):**
- El G3.pdf dice que la capa Bronze se guarda en **Parquet particionado, con Delta Lake** (para tener versionado y poder hacer rollback). Nosotros ahora mismo guardamos los ficheros crudos tal cual llegan (CSV/XML/JSON), no en Parquet/Delta Lake. Fue una decisión consciente de ayer (para no tocar el dato original), pero formalmente no es lo que describe la arquitectura del documento. Habrá que decidir en equipo si convertimos a Parquet ya en Bronze o si se deja para el paso a Silver.
- El G3.pdf pide "validación de esquema en el punto de ingesta: tipos de datos, nulos y rangos aceptables". Ahora mismo los scripts solo descargan y guardan, no validan nada.

### 3. Pendientes de ayer: qué he podido avanzar

- **Tabla INE por provincia**: lo he intentado hoy. Busqué en la operación ECP (la que sustituyó al padrón anual) una tabla con desglose por provincia y no la encontré vía la API — la única tabla de provincias que aparece (`2852`) pertenece a la operación vieja (DPOP) y está igual de descontinuada en 2021 que la que ya sustituimos. Sigue pendiente, hace falta investigar más (puede que esté en otra operación del INE que no he mirado todavía).
- **Clave de AEMET**: sigue pendiente, hay que pedirla por email, no es algo que se resuelva desde el código.
- **Plan B para puntualidad de Renfe**: sigue pendiente, es una decisión de equipo, no técnica.
- **Test sencillo para la ingesta**: sigue pendiente, no lo he tocado hoy — nos hemos centrado en probar que lo que ya había funciona.

### 4. Siguiente paso: subir la rama a GitHub

La rama `feat_ingesta_datos` tiene ya 2 commits y no se ha subido nunca a GitHub (no existe todavía en remoto). Según el README, aquí no se puede hacer commit directo ni a `main` ni a `develop` — hay que pasar por Pull Request con al menos 1 aprobación de otro compañero del equipo antes de fusionar. Queda pendiente abrir esa PR.
