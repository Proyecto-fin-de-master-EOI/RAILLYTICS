# Gobierno de datos: glosario, cálculo, linaje y usos

Para qué sirve este documento: que cualquiera que mire una cifra del cuadro de mandos pueda contestar
**qué significa, de dónde sale, cómo se calcula y qué no se puede concluir de ella** sin leer el
código. Es el diccionario de datos que pide OB5.

Hay dos reglas que atraviesan todo el proyecto y conviene leerlas antes del glosario:

1. **No todo el dato es real, y está marcado.** Hay tres categorías —real, derivado y sintético— y
   cada concepto del glosario dice cuál es. Presentar un dato sintético como real sería el error más
   grave que se puede cometer con este cuadro de mandos.
2. **El NAP da trenes, no plazas.** Cualquier cálculo de capacidad arrastra ese caveat, y es la causa
   de la anomalía documentada en el fin de semana.

- [Glosario](#glosario)
- [Qué es real y qué no](#qué-es-real-y-qué-no)
- [Linaje](#linaje)
- [Casos de uso](#casos-de-uso)
- [Errores de interpretación que ya han pasado](#errores-de-interpretación-que-ya-han-pasado)
- [Atribución obligatoria](#atribución-obligatoria)

---

## Glosario

### Coeficiente de ocupación

**Qué es:** qué parte de la capacidad ofrecida se ha vendido, ponderada por distancia.

```
ocupación = viajeros·km / plazas·km
```

**Cómo se calcula:** numerador del reparto diario de la DTC; denominador, trenes del NAP × plazas·km
por tren de CNMC. Tabla `gold_fact_ocupacion_diaria`.

**Por qué NO es `viajeros / plazas`:** un asiento se vende por tramos —alguien viaja Madrid–Zaragoza
y otro Zaragoza–Barcelona en la misma plaza—, así que el ratio de cabezas **pasa del 100 %
legítimamente** y no sirve para detectar lo imposible. Medido en km, el 100 % sí es un techo real.

**Qué no se puede concluir:** es **mitad real**. El total del trimestre es de CNMC, pero el reparto
entre días lo hacen las reglas del proyecto, así que la ocupación de un día concreto no es una
medición. Sirve para ver forma y orden de magnitud, no para afirmar la ocupación del 14 de marzo.

**Usado en:** cuadro de mandos de oferta y ocupación; objetivo del modelo del Sprint 2.

### Viajeros por plaza ofertada

**Qué es:** `viajeros / plazas_ofertadas` del trimestre, de CNMC.

**No es ocupación.** Puede pasar del 100 % y lo hace. Está en el cuadro de mandos de mercado porque
es el indicador que publica CNMC, no porque mida ocupación. Para ocupación, el concepto anterior.

### Oferta diaria

**Qué es:** cuántos **trenes** circulan cada día en el corredor, por operador.

**Cómo se calcula:** se cuentan los viajes del GTFS que el NAP publica cada día, filtrando los que
salen de Madrid o terminan en Madrid y paran en el destino del corredor. Tabla
`gold_fact_oferta_diaria`.

**Es la única serie diaria medida del proyecto**, y con ella se calibró `base_dia_semana`.

**Qué no se puede concluir:** **son trenes, no plazas.** Una composición doble cuenta como un tren,
así que esto es *servicio* y no *capacidad*. Y falta Iryo, que no publica horarios en el NAP: la
oferta cubre Renfe y OUIGO, no el corredor completo.

### Cobertura completa

**Qué es:** marca booleana de los días en que están todos los operadores que sí publican en el NAP.

**Para qué:** un día al que le falta un operador **no es menos servicio, es dato incompleto**.
Contarlo fingiría un recorte de oferta que no existió. Cualquier agregado de oferta debe filtrar por
`cobertura_completa` o decir que no lo hace.

### DTC — Desagregación Temporal Calibrada

**Qué es:** el método que convierte el total **trimestral** de viajeros de CNMC en una serie
**diaria**.

**Cómo funciona:** a cada día se le asigna un índice con las reglas de `config/reglas_demanda.yml`,
los índices se reescalan a media 1,0 y se reparte el total del trimestre en proporción. La
recalibración es exacta (se hace con fracciones), así que **la suma de los días reproduce el total de
CNMC sin error de redondeo**.

**Consecuencia importante:** como el reparto se normaliza, **un factor común dentro de un trimestre se
cancela**. El nivel absoluto de un coeficiente no significa nada; solo importan las proporciones
entre días.

### Índice de reparto diario

**Qué es:** el peso de un día dentro de su trimestre. 1,00 = un martes o miércoles laborable sin
festivo ni evento.

**Cómo se calcula:**

```
índice = base_dia_semana[día]  (o festivo_entre_semana, o puente)
       + ajustes de víspera y regreso
       × base_mes[mes]
       × factor del evento (si el LLM valora el día)
```

**Dónde está:** `gold_fact_prediccion_demanda.indice`, y el `motivo` de cada día explica en texto qué
reglas se aplicaron.

### Coeficientes de las reglas, y cuáles están medidos

| coeficiente | ¿medido? | con qué |
| --- | --- | --- |
| `base_dia_semana`, lunes a jueves | **sí** | oferta del NAP |
| `base_mes` | **sí** | turismo mensual del INE |
| `base_dia_semana`, viernes, sábado y domingo | **no** | la oferta no distingue demanda alta de capacidad topada, y da trenes y no plazas |
| `festivo_entre_semana`, `puente`, víspera, regreso | **no** | confundidos con la estacionalidad trimestral |
| factores de eventos | **no** | 63 días en un corredor y no existe el contrafactual |

Cada valor lleva su procedencia escrita al lado en `config/reglas_demanda.yml`. Los no medidos son
**hipótesis documentadas**, y lo son porque se intentó estimarlos y las fuentes no contienen la
información necesaria (ver `docs/modelo-ocupacion.md`).

### Cuota del trimestre (turismo)

**Qué es:** qué parte de los turistas de un trimestre cae en cada uno de sus tres meses. Un mes neutro
vale 1/3.

**Cómo se calcula:** `turistas del mes / turistas del trimestre`, y **solo en trimestres con sus tres
meses**: con enero y febrero pero sin marzo, enero saldría con el 50 % y el perfil quedaría al revés.
Los incompletos van a nulo.

**De dónde:** INE, operación TMOV, tabla 53267, estimada por **posición de teléfonos móviles**.

**Qué no se puede concluir:** son viajes en **todos los medios de transporte**, y «turista» en TMOV
implica pernoctación, así que la ida y vuelta en el día no entra. **Se usa la forma, no el nivel.**
Presentar estas cifras como viajeros de tren sería desnaturalizar el dato, que es justo lo que prohíbe
la licencia del INE.

### Cuota de operador

**Qué es:** parte de los viajeros del corredor que lleva cada operador.

**Cómo se calcula:** de la columna `Empresa` del CSV de indicadores de CNMC, media de los ocho
trimestres más recientes. Solo ocho porque el corredor se liberalizó por tramos (OUIGO en 2021, Iryo
en diciembre de 2022) y promediar desde 2016 mezclaría un monopolio con un mercado de tres.

**Qué no se puede concluir:** el reparto **entre Renfe y Avlo es un supuesto**. CNMC las agrega bajo
«Renfe Viajeros» porque son la misma compañía.

### `plausible`

**Qué es:** marca de los días cuya ocupación calculada **no pasa del 100 %**.

**Para qué:** los días imposibles no se borran, se marcan. Y hay que saber que **21 de los 25 caen en
fin de semana**: no es un fallo aleatorio de la fuente, es el problema de trenes contra plazas. Un
agregado de ocupación debería filtrar por `plausible` o decir que no lo hace.

### Nivel esperado del trimestre

**Qué es:** el total de viajeros que se espera en el trimestre que se predice, cuando CNMC todavía no
lo ha publicado (publica con unos seis meses de retraso).

**Cómo se calcula:** del mismo trimestre del año anterior aplicando el crecimiento observado.

### Carga, `run_id` y trazabilidad

**Qué es:** cada ejecución que escribe en el lago deja una fila por tabla en
`raillytics-gold/_trazabilidad/cargas/`, con proceso, capa, origen, destino, filas, duración y estado.
Los resultados de quality gates se registran con el **mismo `run_id`**.

**Para qué:** poder contestar «¿de qué ejecución salió esta cifra y qué gates pasó?». Es el cuadro de
mandos de trazabilidad.

### Quality gate

**Qué es:** una comprobación declarada en `config/quality_gates.yml` que se evalúa sobre una tabla
antes de escribir o publicar.

**Severidades:** `bloqueante` aborta la carga; `aviso` se registra y deja pasar.

**Dónde actúan:** en la descarga (que el fichero *sea* lo que dice ser; lo que no pasa va a cuarentena
en `data/bronze_rejected/`), a la entrada de Gold (contrato de Silver) y a la salida de Gold (sobre lo
construido, antes de sobrescribir nada).

### `tipo_dato` (INE)

Definitivo, provisional o avance. El INE marca los meses recientes como provisionales; se guardan en
vez de descartarlos para que quien calcule pueda decidir.

### `snapshot` (NAP)

Identificador del fichero de horarios del que salió el recuento de un día. El NAP publica un snapshot
por día y el histórico se reconstruye pidiéndolos uno a uno.

---

## Qué es real y qué no

| pieza | categoría | de dónde sale |
| --- | --- | --- |
| Mercado trimestral (viajeros, plazas, precios, ingresos) | **real** | CNMC, de principio a fin |
| Oferta diaria (trenes/día) | **real** | NAP, horarios publicados |
| Festivos | **real** | BOE, calendario laboral |
| Meteorología observada | **real** | AEMET |
| Turismo mensual | **real** | INE, operación TMOV |
| Cuota de operador | **real** | CNMC, columna `Empresa` |
| Reparto diario de la demanda | **derivado** | total real de CNMC repartido por reglas |
| Coeficiente de ocupación diaria | **derivado** | reparto derivado ÷ oferta real |
| Total diario del corredor | **real** | CNMC, viajeros del trimestre entre sus días |
| `viajeros_enriquecidos` (reparto por par origen-destino) | **sintético**, sobre el total real del corredor y con la cuota de operador real | nadie publica viajeros por par |
| `puntualidad_enriquecida` | **sintético** | el GTFS-RT se ingiere y nadie lo consume todavía |

Las dos sintéticas no son un descuido: son el andamio que permitió desarrollar Gold y los cuadros de
mandos antes de tener las fuentes. **Pero dos hechos de Gold se apoyan en ellas**, y eso hay que
decirlo al presentar cualquier cifra por estación o por trayecto.

Lo que cambió el 17 de octubre de 2026 es **cuánto hay de andamio**: el total que se reparte es el de
la CNMC y el grano es el par origen-destino, así que lo sintético se reduce al reparto entre pares.
Y ese reparto deja una comprobación que antes no existía: los viajeros·km calculados desde abajo
(viajes × distancia del par) contra los que publica la CNMC.

---

## Linaje

El recorrido general, con los ficheros que intervienen en cada paso, está en `docs/arquitectura.md`.
Por concepto:

```
Coeficiente de ocupación diaria
  CNMC (csv) ──► l1-raw ──► l2 (Parquet) ──► silver_cnmc_trimestral ──┐
                                                                      ├─► silver_ml_ocupacion_diaria
  NAP (zip GTFS) ──► data/historico/nap ──► silver_oferta_diaria ─────┘        (make ml-dataset)
  BOE (xml) ──► silver_festivos ──────────────────────────────┐                       │
  AEMET (json) ──► silver_meteo ──────────────────────────────┘                       ▼
                                                                        gold_fact_ocupacion_diaria
                                                                                      │
                                                                                      ▼
                                                            dataset «ocupacion_diaria» (Superset)

base_mes (coeficiente del reparto mensual)
  INE TMOV (json) ──► silver_turismo_mensual ──► make perfil-mensual ──► config/reglas_demanda.yml
                                            └──► gold_fact_turismo_mensual ──► Superset

Reparto diario de la demanda
  silver_cnmc_trimestral ──► nivel esperado ──┐
  silver_festivos, eventos, silver_meteo ─────┼─► reglas + LLM (eventos) ──► gold_fact_prediccion_demanda
  config/reglas_demanda.yml ──────────────────┘
```

**Cada paso queda registrado** en la trazabilidad de cargas con su `run_id`, así que el linaje no es
solo este dibujo: se puede reconstruir desde los datos.

---

## Casos de uso

| pregunta de negocio | dónde se contesta | con qué cuidado |
| --- | --- | --- |
| ¿Cómo evoluciona la demanda del corredor y el reparto entre operadores? | Mercado del corredor | dato real; Avlo va dentro de Renfe |
| ¿Cuánto servicio pone cada operador y cómo varía por día de la semana? | Oferta y ocupación | trenes, no plazas; falta Iryo |
| ¿Qué días van más llenos? | Oferta y ocupación | la ocupación es derivada; filtrar por `plausible` |
| ¿Qué demanda se espera los próximos días? | Predicción de demanda | el reparto lo hacen reglas, y dos de sus coeficientes son hipótesis |
| ¿Qué meses concentran el viaje Madrid–Barcelona? | Oferta y ocupación (perfil mensual) | son turistas de todos los medios; se usa la forma, no el nivel |
| ¿Los datos de hoy están completos y han pasado los controles? | Trazabilidad de cargas | — |
| ¿Qué precio medio tiene el trayecto y cómo evoluciona? | Mercado del corredor | dato real de CNMC |
| ¿Cómo va la puntualidad? | Puntualidad | **sintético**: no se puede usar para concluir nada del servicio real |

---

## Errores de interpretación que ya han pasado

Se dejan escritos porque son los que de verdad se cometen, y los tres se cometieron aquí:

1. **Confundir `viajeros/plazas` con ocupación.** Daba un 111 % de media trimestral y pareció un bug
   de datos. No lo era: el ratio de cabezas pasa del 100 % porque un asiento sirve a varios tramos.
2. **Dar por fallo de la fuente un sesgo sistemático.** Los días con ocupación imposible se
   atribuyeron a fallos del conteo del NAP. Al desglosarlos por día de la semana, 21 de 25 eran fin de
   semana: era el problema de trenes contra plazas.
3. **Usar el MAPE del modelo para elegir coeficientes del reparto.** Es circular: el objetivo lo
   generan esas mismas reglas, así que aplanar un coeficiente mejora el MAPE sin que eso diga nada de
   si el coeficiente es correcto. El código avisa cuando el MAPE baja del 5 % por esta razón.

---

## Atribución obligatoria

Dos licencias de origen exigen un texto concreto. El detalle completo está en `docs/licencias.md`.

- **CNMC (CC BY-SA 4.0)**, y obliga además a **compartir igual** las obras derivadas:
  > Origen de los datos: Comisión Nacional de los Mercados y la Competencia
- **INE (CC BY 4.0)**, con el texto de datos tratados:
  > Elaboración propia con datos extraídos del sitio web del INE: www.ine.es
- **AEMET**: reutilización autorizada citando a AEMET como autor.
- **BOE**: disposiciones oficiales, sin restricción; se cita el documento para identificar la versión.
- **NAP**: no publica condiciones propias (su aviso legal devuelve 404 y su GTFS no trae
  `feed_info.txt`), así que se le aplica el **régimen general** de reutilización de información del
  sector público —Ley 37/2007 y RD 1495/2011—, cuyas cuatro condiciones el proyecto cumple. El
  detalle, en [licencias.md](licencias.md).
