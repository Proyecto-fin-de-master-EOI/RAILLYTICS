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
| NAP (Mº Transportes) | sin condiciones propias; **régimen general** (Ley 37/2007) | **sí** | no |
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

## NAP — sin condiciones publicadas, se aplica el régimen general

El Punto de Acceso Nacional del Ministerio de Transportes existe por el Reglamento Delegado (UE)
2017/1926, que obliga a los estados a publicar la información de transporte de viajeros en formato
digital para que terceros la usen. Esa es la finalidad declarada.

**Pero el NAP no publica condiciones de uso propias.** Comprobado el 18 de octubre de 2026, una por
una:

| dónde se buscó | qué responde |
| --- | --- |
| `nap.transportes.gob.es/aviso-legal` | **404** |
| `nap.transportes.gob.es/Home/AvisoLegal` | **404** |
| `www.transportes.gob.es/aviso-legal` | **403** (no se puede leer desde fuera) |
| `nap.transportes.gob.es` (portada) | no se pudo descargar para buscar el enlace: la respuesta pasa de 10 MB |
| el propio GTFS que descargamos | **no trae `feed_info.txt`**, que es el fichero del estándar donde se declaran el editor del feed y su licencia. El ZIP solo trae `agency.txt`, `calendar.txt`, `calendar_dates.txt`, `routes.txt`, `stops.txt`, `stop_times.txt` y `trips.txt` |

Así que no hay licencia declarada que citar, y no por no haberla buscado.

**Qué se aplica entonces.** El NAP es información del sector público de la Administración General del
Estado, y para eso hay régimen general: la **Ley 37/2007** de reutilización de la información del
sector público y el **Real Decreto 1495/2011** que la desarrolla para el sector público estatal. Es
el mismo marco que el portal `datos.gob.es` declara en su aviso legal, que sí es legible, y que
permite la reutilización —comercial y no comercial— con cuatro condiciones:

1. citar la fuente;
2. no desnaturalizar la información;
3. mencionar la fecha de la última actualización si figura en el original;
4. no dar a entender que el titular de los datos patrocina o apoya la reutilización.

**Esto es una inferencia, y conviene decirlo así en la defensa**: ninguna página del NAP dice que sus
horarios se publiquen bajo ese régimen. Lo que se afirma es que es información pública de la AGE y
que, a falta de condiciones específicas, el régimen general es el que le corresponde por ley.

**Qué hace el proyecto para cumplirlo.** Las cuatro condiciones se cumplen y se puede enseñar dónde:

- **Se cita la fuente** en el `NOTICE`, en el README y al pie de los cuadros de mando que enseñan
  oferta.
- **No se desnaturaliza**: de los horarios solo se cuentan trenes por día, corredor y operador. El
  dato no se modifica, se agrega, y el aviso de que **el NAP da trenes y no plazas** acompaña a
  cualquier cifra de ocupación en `docs/arquitectura.md` y en los propios cuadros.
- **La fecha queda registrada**: cada descarga deja su `snapshot` en `fact_oferta_diaria` y su fila
  en la trazabilidad de cargas, con fecha y hora.
- **No se sugiere patrocinio**: el repositorio dice en el README que es un trabajo académico y que el
  Ministerio no participa en él.

Y una condición que el proyecto se impone por prudencia, no porque se la exijan: **no se redistribuye
el GTFS en crudo**. Los ZIP descargados se quedan en el lago, que no está versionado; lo que viaja al
repositorio son agregados (trenes por día) y resultados derivados.

> Horarios: Punto de Acceso Nacional de Transporte Multimodal (Ministerio de Transportes)

**Cómo se cerraría del todo**, si alguien quiere ir más lejos: escribiendo al NAP por su formulario
de contacto y pidiendo las condiciones por escrito, o releyendo los términos que se aceptaron al dar
de alta la clave de API (`NAP_API_KEY`), que es el otro sitio donde una administración suele ponerlos.
Ninguna de las dos cosas es un bloqueo para entregar: el régimen general aplica igual.

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

**3. Las condiciones del NAP, cerradas el 18 de octubre de 2026.** No publica condiciones propias
—su aviso legal devuelve 404, el del Ministerio 403 y su GTFS no trae `feed_info.txt`—, así que se
aplica el régimen general de reutilización de información del sector público (Ley 37/2007 y RD
1495/2011) y el proyecto cumple sus cuatro condiciones. El razonamiento completo, con lo que se
comprobó y dónde se cumple cada una, está en el apartado del NAP de este mismo documento. Se deja
dicho que es una inferencia: el NAP no declara ese régimen, se le aplica por ser información pública
de la AGE.

## Una nota sobre quién es el titular

El `LICENSE` pone el copyright a nombre del proyecto y sus contribuidores —no de una persona—, porque
el repositorio tiene trabajo de varias. **Elegir la licencia del código es una decisión del equipo**,
no de quien escribe el fichero: afecta a la obra de todos. Si alguien prefiere otra (Apache 2.0 añade
una concesión expresa de patentes, por ejemplo), es cambiar un fichero.
