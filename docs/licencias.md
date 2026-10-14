# Licencias de las fuentes y atribución

Qué licencia tiene cada dato que entra en el proyecto y a qué obliga. Importa porque **el
repositorio es público**: lo que se publica aquí son obras derivadas de datos de terceros, y algunas
de esas licencias imponen condiciones que hay que cumplir.

## Resumen

| fuente | licencia | ¿atribución? | ¿compartir igual? |
| --- | --- | --- | --- |
| CNMC | **CC BY-SA 4.0** | **sí, con texto concreto** | **sí** |
| INE | **CC BY 4.0** | **sí, con texto concreto** | no |
| AEMET | reutilización autorizada citando a AEMET | **sí** | no |
| BOE | disposiciones oficiales, sin derecho de autor | recomendable | no |
| NAP (Mº Transportes) | **pendiente de confirmar** | — | — |
| Renfe GTFS-RT, CRTM | **pendiente de confirmar** | — | — |

---

## CNMC — la que más obliga

**Licencia: Creative Commons Reconocimiento-CompartirIgual 4.0 Internacional (CC BY-SA 4.0).**

El catálogo da tres respuestas distintas y conviene saber cuál manda:

| campo | dice |
| --- | --- |
| `license_id` | `CC-BY-SA-4.0` |
| `extras.access_rights` | apunta a ODbL |
| `extras.license` | apunta a [data.cnmc.es/condiciones-de-uso](https://data.cnmc.es/condiciones-de-uso) |

La última es la autorizada, y esa página dice literalmente: *«Esta obra está bajo una licencia de
Creative Commons Reconocimiento-CompartirIgual 4.0 Internacional»*. El ODbL está en un campo que es
para condiciones de acceso, no para la licencia: es un error de metadatos de la CNMC.

**A qué obliga:**

1. **Atribución, con este texto exacto:**
   > Origen de los datos: Comisión Nacional de los Mercados y la Competencia
2. **Compartir igual.** Las obras derivadas se distribuyen bajo la misma licencia. Aquí son
   derivadas las tablas de Silver y Gold construidas desde sus CSV, y los gráficos del dashboard.
3. **Citar la fecha de última actualización** y conservar los metadatos de reutilización.
4. **No desnaturalizar la información.**

**Qué hay en el proyecto que sea obra derivada:** `silver/cnmc_trimestral`,
`silver/cnmc_precio_trimestral`, `silver/cnmc_precio_mensual`, los hechos de Gold que salen de
ellas, y la ocupación calculada en `ml_ocupacion_diaria`.

## INE — la segunda que obliga a un texto concreto

**Licencia: Creative Commons Reconocimiento 4.0 Internacional (CC BY 4.0).**

Tiene el mismo tipo de trampa que la CNMC, y conviene saber distinguirlo: el pie del sitio web del
INE dice *«Este sitio web y su contenido están bajo licencia CC BY-SA 4.0»*, pero eso se refiere al
**sitio**. Su [aviso legal](https://ine.es/dyngs/AYU/index.htm?cid=125) es explícito sobre los datos:
*«La licencia de uso general a aplicar a la **información estadística** de este sitio web, salvo que
se indique lo contrario, es la Creative Commons Reconocimiento 4.0 (CC BY 4.0)»*.

La diferencia importa: **CC BY 4.0 no obliga a compartir igual**. Al contrario que la CNMC, el INE no
añade ninguna obligación de copyleft al proyecto.

**A qué obliga:**

1. **Atribución, y el texto depende de si se tratan los datos.** Aquí se tratan —se calculan cuotas
   mensuales a partir de la serie—, así que el texto que exige el INE es el segundo:
   > Elaboración propia con datos extraídos del sitio web del INE: www.ine.es
2. **Citar la fecha de última actualización** cuando venga en el original.
3. **No desnaturalizar el sentido de la información.** Importa en este caso concreto: TMOV mide
   turistas en todos los medios de transporte y con pernoctación, y el proyecto solo usa la FORMA de
   la serie (el peso de cada mes), nunca su nivel como si fuera demanda ferroviaria. Presentarla como
   viajeros de tren sería exactamente lo que la licencia prohíbe.
4. **No se puede dar a entender que el INE patrocina o apoya** el trabajo.

**Qué hay en el proyecto que sea obra derivada:** `silver/turismo_mensual` y los coeficientes
`base_mes` de `config/reglas_demanda.yml`, que salen de promediar esas cuotas.

## AEMET

La reutilización está autorizada **citando a AEMET como autor**. Las condiciones generales están en
su [nota legal](https://www.aemet.es/es/nota_legal).

Atribución sugerida:

> Datos meteorológicos: © AEMET (Agencia Estatal de Meteorología)

## BOE

Lo que se usa del BOE son **disposiciones oficiales** —las resoluciones anuales del calendario
laboral—. El artículo 13 del texto refundido de la Ley de Propiedad Intelectual deja fuera de la
protección del derecho de autor las disposiciones legales y reglamentarias y sus textos oficiales,
así que no hay restricción de reutilización.

Aun así conviene citar el documento concreto, porque identifica la versión usada:

> Festivos: resoluciones de la Dirección General de Trabajo publicadas en el BOE
> (BOE-A-2024-21316 para 2025, BOE-A-2025-21667 para 2026)

## NAP — pendiente de confirmar

El Punto de Acceso Nacional del Ministerio de Transportes existe por el Reglamento Delegado (UE)
2017/1926, que obliga a los estados a publicar la información de transporte de viajeros en formato
digital para que terceros la usen. Esa es la finalidad declarada.

**Pero no se ha localizado la página de condiciones de uso**: `nap.transportes.gob.es/aviso-legal`
devuelve 404. Hasta confirmarlo, lo prudente es citar la fuente igualmente:

> Horarios: Punto de Acceso Nacional de Transporte Multimodal (Ministerio de Transportes)

**Esto hay que cerrarlo** antes de entregar, sobre todo porque la serie de oferta diaria se construye
desde ahí y es una pieza central del trabajo.

## Renfe GTFS-RT y CRTM — pendiente de confirmar

Son las fuentes de ejemplo del framework. Si acaban usándose en el entregable, hay que mirar sus
condiciones.

---

## Cómo queda resuelto en el repositorio

**1. El código, bajo MIT** — [`LICENSE`](../LICENSE). El repositorio es público y hasta el 12 de
octubre de 2026 no tenía ninguna licencia, lo que en términos de derecho de autor significa «todos
los derechos reservados»: nadie podía reutilizar legalmente el código.

Son dos cosas distintas y conviene no mezclarlas:

- **El código** (Python, Scala, SQL, configuración) es obra propia y **no deriva** de los datos de la
  CNMC, así que puede llevar una licencia permisiva. Se elige **MIT** por ser la más simple y la
  habitual en un proyecto académico.
- **Los datos derivados** (las tablas de Silver y Gold que salen de la CNMC, la ocupación diaria y lo
  que muestre el cuadro de mandos) **van bajo CC BY-SA 4.0**, porque la licencia de origen lo impone.

El `LICENSE` es el texto MIT **literal y nada más**, a propósito: GitHub detecta la licencia con un
comparador de texto, y cualquier añadido —aunque sea un párrafo aclaratorio— hace que la deje de
reconocer. Por eso las condiciones de los datos van en [`NOTICE`](../NOTICE), que es el sitio
convencional para esto.

**2. La atribución, visible en tres sitios.** Antes solo estaba en un comentario de
`config/data_sources.yml`, que no lo lee nadie que no mire el código. Ahora está en:

- el [`NOTICE`](../NOTICE), con el texto literal de cada fuente y qué obliga cada licencia;
- la sección «Licencia» del README, que es lo primero que ve quien entra al repositorio;
- el **cuadro de mandos**, en un bloque de texto al pie de los dashboards que muestran datos de la
  CNMC o del INE, que es donde alguien ve las cifras sin pasar por el repositorio.

**3. Qué sigue pendiente: las condiciones del NAP.** Su página de aviso legal devuelve 404 y no se ha
localizado otra. Es lo único abierto de este apartado, y conviene cerrarlo antes de entregar porque
la serie de oferta diaria se construye desde ahí y es una pieza central del trabajo. Mientras, se
cita la fuente igualmente y queda dicho que las condiciones no están confirmadas.

## Una nota sobre quién es el titular

El `LICENSE` pone el copyright a nombre del proyecto y sus contribuidores —no de una persona—, porque
el repositorio tiene trabajo de varias. **Elegir la licencia del código es una decisión del equipo**,
no de quien escribe el fichero: afecta a la obra de todos. Si alguien prefiere otra (Apache 2.0 añade
una concesión expresa de patentes, por ejemplo), es cambiar un fichero.
