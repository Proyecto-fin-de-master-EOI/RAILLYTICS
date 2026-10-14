# Modelo de ocupación diaria: qué se hizo y qué salió

Experimento de modelado del coeficiente de ocupación diaria del corredor Madrid–Barcelona, que es
lo que pide el Sprint 2 («Silver enriquecido para un GBTRegressor que prediga el coeficiente de
ocupación a 5 días con MAPE ≤ 15 %»).

**Resumen:** el modelo cumple el umbral de MAPE y, desde que el reparto diario incorpora un perfil
mensual medido, **mejora a la referencia trivial por poco** (10,1 % frente a 10,3 %). Antes de eso
perdía (12,0 % frente a 11,4 %). La lección del experimento no cambió por ganar: lo que mueve el
resultado no es el modelo ni las variables, es **la calidad del reparto que construye el objetivo**.
Se explica abajo con los números, incluido el A/B.

- [Qué se predice](#qué-se-predice)
- [Cómo se construye el objetivo](#cómo-se-construye-el-objetivo)
- [Decisiones de diseño](#decisiones-de-diseño)
- [Resultados](#resultados)
- [Qué aporta cada variable](#qué-aporta-cada-variable)
- [Por qué sale así](#por-qué-sale-así)
- [Lo que sí cambió el resultado: el perfil mensual](#lo-que-sí-cambió-el-resultado-el-perfil-mensual)
- [Limitaciones](#limitaciones)

---

## Qué se predice

El **coeficiente de ocupación** de un día: `viajeros·km / plazas·km`.

No el ratio de cabezas (`viajeros / plazas`), que pasa del 100 % legítimamente porque un asiento se
vende por tramos: alguien viaja Madrid–Zaragoza y otro Zaragoza–Barcelona en la misma plaza. Medido
en km, el 100 % sí es un techo real, y eso lo hace utilizable como objetivo y como control.

Código: `python/raillytics/ml/dataset.py` y `python/raillytics/ml/modelo.py`.

## Cómo se construye el objetivo

No existe ocupación diaria observada: nadie publica viajeros por día. Se construye cruzando dos
fuentes, y el resultado es **mitad real**:

| parte | de dónde sale | ¿es real? |
| --- | --- | --- |
| numerador — viajeros·km del día | reparto diario de la DTC | **no**: el total del trimestre es real (CNMC), pero el reparto entre días lo hacen reglas |
| denominador — plazas·km del día | trenes que circularon (NAP) × plazas·km por tren (CNMC) | **sí**: los trenes están medidos |

Solo entran los trimestres con **al menos un 90 % de días con oferta**. Las plazas por tren salen de
dividir las plazas·km del trimestre completo entre los trenes observados: con cobertura parcial
salen muchas más plazas por tren de las que hay y la ocupación se desploma. Con 18 días de 92,
2025-T4 daba un 15 % de ocupación media, que es imposible.

Quedan **180 días** (2026-T1 y 2026-T2), de los que **155 son utilizables**: 25 se marcan
`plausible = False` porque dan una ocupación por encima del 100 %. No se borran, se marcan.

**Y esos 25 no están repartidos al azar**, que es lo que se creía al principio:

| día | imposibles | de | |
| --- | --- | --- | --- |
| lun, jue | 0 | 26 | |
| mar, vie | 1 | 26 | |
| mié | 2 | 25 | uno de ellos al 277 %: ahí sí falla el conteo del NAP |
| **sáb** | **10** | 25 | **40 %** |
| **dom** | **11** | 26 | **42 %** |

**21 de 25 caen en fin de semana.** Eso no es un fallo aleatorio de la fuente, es un sesgo
sistemático, y la causa más probable es el aviso que arrastra todo el proyecto: **el NAP da trenes, no
plazas**. Las plazas por tren se calculan como una constante trimestral, así que si el operador pone
composiciones dobles los días de ocio —práctica habitual—, la capacidad del fin de semana sale corta y
la ocupación sale alta. Cuantificado: haría falta un **1,26× en la capacidad del domingo** para que
ningún domingo pase del 100 %, lo que equivale a que una cuarta parte de los trenes lleven doble
composición.

**La consecuencia práctica importa para los coeficientes:** la ocupación implícita del fin de semana
**no sirve** para decidir `sáb` y `dom`, porque el error del denominador se concentra justo ahí.
Bajarlos para que «cuadre» sería arreglar un error de medición de la capacidad distorsionando un
parámetro de demanda. Para los días laborables el contraste sí es utilizable (0-8 % de imposibles).

## Decisiones de diseño

**Fuera la oferta y fuera el índice de la DTC.** Son el denominador y el numerador del objetivo. Si
entran como variables, el modelo reconstruye el objetivo en vez de aprenderlo y el error sale casi
cero sin significar nada.

**Partición temporal, no aleatoria.** Es una serie en el tiempo: con una partición al azar el modelo
vería días de junio para predecir días de mayo y el error saldría mejor de lo que será.

**Las categorías del día de la semana se fijan a los siete.** Si se dejan deducir de cada conjunto,
un tramo de evaluación sin ningún domingo no tiene esa columna y no encaja con el modelo entrenado.

**scikit-learn y no la clase `GBTRegressor` de Spark.** Es el mismo algoritmo —gradient boosted
trees— con otro nombre según la biblioteca. Se usa scikit-learn porque es lo que declara el stack
del proyecto (ver el README) y porque repartir 157 filas entre ejecutores de Spark cuesta más que
calcularlas. Dentro de scikit-learn se elige `HistGradientBoostingRegressor` porque **admite valores
ausentes de forma nativa**: hay 16 días sin temperatura porque AEMET no la publicó, y la ingesta los
deja vacíos en vez de inventar un cero; imputar aquí contradiría esa decisión.

## Resultados

Con una sola partición (los últimos 31 días como evaluación):

```
MAPE 8.0 %  ·  error medio 6.6 puntos de ocupación  ·  R² −0.10
```

Cumple el umbral del plan. **Pero ese número es frágil**: depende de qué semanas tocaron, y el R²
negativo ya avisa de que sobre ese corte concreto el modelo no explica la varianza. Con validación
temporal en cinco tramos, comparando siempre contra la referencia trivial —predecir la media de lo ya
visto—:

| tramo | días evaluados | MAPE | MAPE de la referencia | R² | ¿gana? |
| --- | --- | --- | --- | --- | --- |
| 1 | 1-feb a 26-feb | 11,4 % | 11,4 % | −0,00 | no |
| 2 | 27-feb a 24-mar | 11,3 % | 9,5 % | −1,06 | no |
| 3 | 25-mar a 20-abr | 12,2 % | 12,4 % | −0,94 | sí |
| 4 | 21-abr a 22-may | 8,1 % | 9,3 % | −0,18 | sí |
| 5 | 25-may a 30-jun | 7,4 % | 9,0 % | 0,00 | sí |
| **media** | | **10,1 %** | **10,3 %** | | **3 de 5** |

**El modelo gana a predecir la media, pero por 0,2 puntos.** Con 155 días y unos 26 por tramo, esa
diferencia no es concluyente: lo honesto es decir que empata con la referencia trivial, no que la
bate.

Lo que sí es un cambio real está en el tramo 3, **Semana Santa**, que antes era el desastre del
experimento (17,4 % frente al 9,9 % de la referencia) y ahora empata (12,2 % frente a 12,4 %). El
perfil mensual le da al modelo con qué anticipar que marzo y abril no están al mismo nivel.

## Qué aporta cada variable

Entrenando sin cada grupo y midiendo cuánto empeora el MAPE medio de los cinco tramos:

```
con todas las variables      10,1 %   (referencia trivial 10,3 %)
solo el día de la semana     10,8 %
```

| grupo | columnas | MAPE sin él | cambio | |
| --- | --- | --- | --- | --- |
| **mes** | 1 | 10,4 % | **+0,3 pp** | **aporta** |
| día de la semana | 7 | 10,1 % | +0,0 pp | da igual |
| festivos | 3 | 10,1 % | +0,0 pp | da igual |
| vísperas y puentes | 3 | 10,1 % | +0,0 pp | da igual |
| eventos | 7 | 10,1 % | +0,0 pp | da igual |
| **meteo** | 4 | **9,3 %** | **−0,8 pp** | **estorba** |

**El mes es la primera variable que aporta algo**, y es exactamente la que acaba de recibir un dato
medido detrás (ver la sección siguiente). Festivos, eventos y vísperas siguen en +0,0 pp.

**La meteorología no es que dé igual: estorba.** Quitarla mejora el MAPE en 0,8 pp y deja el mejor
resultado de todo el experimento (9,3 % frente al 10,3 % de la referencia). El signo es el mismo que
la vez anterior (−0,2 pp), medido sobre dos versiones distintas del objetivo, así que no parece
casualidad. La temperatura de Madrid, además, aparecía con un 15 % en la importancia por permutación
de una partición única: **era ruido**, y es un buen recordatorio de que esa métrica sobre un solo
corte no demuestra nada.

Dicho eso, con 26 días por tramo una diferencia de 0,8 pp tampoco es concluyente, así que la meteo
**se deja dentro y documentada** en vez de quitarla: sacar una variable por un resultado de esta
muestra sería elegir el modelo mirando el conjunto de evaluación.

## Por qué sale así

No es un fallo del modelo ni de las variables: **es estructural, y se deduce de cómo se construye el
objetivo**.

La ocupación es `demanda ÷ oferta`. Y se midió cómo se comporta cada parte:

- **La demanda** la reparten reglas que miran día de la semana, festivos y vísperas.
- **La oferta** la pone el operador, y está medido que apenas varía salvo por el día de la semana:
  69 trenes un martes, 46 un sábado, y **plana en festivos y en días con evento** (factor 1,02).

Al dividir una entre otra, **los festivos se cancelan**: suben el numerador y bajan el denominador a
la vez. Lo que sobrevive es el patrón semanal, que es donde sí hay desajuste entre lo que la demanda
pide y lo que la oferta ofrece.

Los eventos no aportan por otra razón: son 63 días de 400, y en ellos la oferta no se mueve. Con tan
pocos casos no hay nada estable que aprender.

**La conclusión honesta:** entrenar sobre la salida de la DTC produce un modelo que confirma la DTC.
La única señal disponible es la que se introdujo al escribir las reglas, y el modelo la encuentra
enseguida porque no hay más.

Eso tiene un corolario que se comprobó después y que cambia por dónde hay que trabajar: **si la
señal del modelo es la que metieron las reglas, mejorar las reglas mejora el modelo**. Es lo que pasó
con el perfil mensual.

## Lo que sí cambió el resultado: el perfil mensual

El total que reparte la DTC es **trimestral**, porque CNMC no publica meses. Las reglas miraban el día
de la semana, los festivos y los puentes, así que arrastraban una hipótesis que nadie había escrito:
**que dentro de un trimestre todos los meses están al mismo nivel**. Un martes de enero y un martes de
marzo recibían el mismo índice.

Es falsa, y hay con qué medirlo. El INE publica mes a mes los turistas que se mueven de Madrid a
Barcelona (operación TMOV, tabla 53267, estimada por posición de teléfonos móviles). Promediando qué
cuota de cada trimestre cae en cada mes, sobre 2019 y 2022-2026:

| mes | cuota del trimestre | t | coeficiente |
| --- | --- | --- | --- |
| mar | 38,1 % | 4,0 | **1,143** |
| jul | 38,1 % | 16,4 | **1,142** |
| oct | 33,9 % | 2,1 | 1,016 |
| feb | 31,9 % | −3,1 | 0,957 |
| abr | 31,4 % | −3,4 | 0,942 |
| ene | 30,0 % | −3,6 | **0,901** |
| ago | 29,1 % | −9,2 | **0,873** |

Los otros cinco meses (may, jun, sep, nov, dic) no se distinguen del tercio neutro con cinco años de
muestra y **se quedan en 1,000 clavado**: medir una diferencia no da derecho a afirmarla.

**El efecto en el experimento, medido con un A/B** (mismo código, mismos días, solo cambia si
`config/reglas_demanda.yml` declara `base_mes`):

| | MAPE validación temporal | referencia trivial | veredicto |
| --- | --- | --- | --- |
| sin perfil mensual | 12,0 % | 11,4 % | **pierde** por 0,6 pp |
| con perfil mensual | **10,1 %** | 10,3 % | **gana** por 0,2 pp |

**Lo que esto demuestra y lo que no.** No demuestra que el modelo haya aprendido algo del mundo: la
variable `mes` predice mejor porque el perfil mensual está en las reglas que generan el objetivo,
así que el modelo está redescubriendo una regla nuestra. Lo que sí demuestra es que **el cuello de
botella está en el reparto, no en el modelado**, y que el reparto se puede mejorar con dato medido en
vez de con hipótesis. El reparto diario de la DTC es hoy más creíble que ayer, y eso vale
independientemente de lo que haga el modelo encima.

## ¿Se pueden inferir los coeficientes en vez de ponerlos a mano?

Los pesos de `config/reglas_demanda.yml` eran todos hipótesis. Dos familias se han podido calibrar y
una no, y la diferencia tiene una causa clara: **hace falta una fuente medida a la misma resolución
que el coeficiente**.

**El día de la semana sí.** Se calibró contra la oferta real del NAP, que da trenes por día.

**El mes también.** Se calibró contra el turismo mensual del INE (ver la sección anterior). Y aquí
está el contraste que explica por qué los festivos no se pudieron: el efecto de los festivos salía con
**t = 0,71** sobre 34 trimestres y el del mes sale con **t = 16,4** para julio y **t = −9,2** para
agosto. No es que haya más datos —son menos años—, es que **la cuota mensual se mide dentro del
trimestre, así que la estacionalidad trimestral ya está descontada**, y era justo la estacionalidad lo
que confundía la estimación de los festivos.

**Los festivos y los puentes no.** Se intentó estimarlos de los **42 trimestres de CNMC**
(2016T1–2026T2), aprovechando que el número de festivos que caen en día laborable cambia de un
trimestre a otro.

Para eso hacía falta el calendario de festivos desde 2016, y solo tenemos el BOE de 2025 y 2026. Se
generó por regla —fechas fijas más las que cuelgan de la Pascua, que se calcula— y **se validó contra
el BOE real**: reproduce 2026 exactamente y 2025 con un día de diferencia (Santiago Apóstol, que es
una elección discrecional de cada comunidad y no se deduce por regla). 31 aciertos de 32.

Modelando `log(viajeros) ~ tendencia + trimestre + nº de festivos laborables` sobre los 34
trimestres utilizables (fuera 2020 y 2021 por la pandemia):

```
efecto de un festivo laborable de más:  +1,36 %
error estándar:                          1,92 pp
t = 0,71   ->  no significativo
```

**El efecto no es distinguible de cero.** Sale además con el signo contrario al esperado, y traducido
al peso de las reglas daría 2,18 frente al 0,70 actual, que es un disparate.

**Por qué no sale.** El número de festivos laborables está confundido con la estacionalidad: los
terceros trimestres tienen siempre 1 o 2, los segundos casi siempre 5 o 6. El modelo no puede separar
si el verano vende más porque es verano o porque tiene pocos festivos, y el efecto estacional es
mucho mayor. El experimento que lo rompería es el movimiento de la Semana Santa entre trimestres,
pero **solo cambia dos veces en once años** (2016 y 2024).

### ¿Y contrastar el día de la semana con el interés de búsqueda?

`base_dia_semana` se calibró contra la **oferta** del NAP, que es capacidad y no demanda; el propio
fichero de reglas dice que «lo medido es el TECHO, no el valor». Así que se buscó un contraste del
lado de la demanda y además independiente: cuánta gente busca el viaje cada día de la semana en
Google Trends (`scripts/contraste_trends.py`).

**Por qué esto se puede hacer y una serie diaria larga no.** Trends normaliza cada descarga poniendo
el máximo de la ventana pedida a 100, así que dos ventanas no son comparables y construir histórico
obliga a encadenarlas por solapes, acumulando error. Pero **un perfil semanal no necesita encadenar
nada**: «¿qué día se busca más?» se contesta dentro de una sola ventana, donde la normalización es la
misma para los siete días. La granularidad la decide el rango pedido —menos de 9 meses da días, más
de 5 años da meses—, así que las ventanas son de 260 días.

**Hay patrón semanal, y es estable.** Tres ventanas independientes, una por año:

| día | 2024 | 2025 | 2026 | media | reglas | oferta |
| --- | --- | --- | --- | --- | --- | --- |
| lun | 1,052 | 1,020 | 1,093 | 1,055 | 1,05 | 0,99 |
| mar | 1,000 | 1,000 | 1,000 | 1,000 | 1,00 | 1,00 |
| mié | 0,981 | 0,946 | 0,979 | 0,969 | 1,00 | 1,03 |
| jue | 0,998 | 0,937 | 1,030 | 0,988 | 1,05 | 1,04 |
| **vie** | 0,930 | 0,937 | 0,955 | **0,941** | **1,15** | 0,96 |
| sáb | 0,897 | 0,907 | 0,971 | 0,925 | 0,80 | 0,68 |
| **dom** | 1,042 | 1,119 | 1,166 | **1,109** | **1,00** | 0,76 |

El domingo es el día de más búsquedas y el viernes y el sábado los de menos, con el mismo signo en
las tres ventanas y significativo en al menos dos cada uno. Las reglas dicen lo contrario de esos dos
días: para ellas el viernes es el más fuerte de la semana y el domingo está en la media.

**Y aun así no sirve de contraste.** Por dos razones medidas:

1. **El desfase entre buscar y viajar no se puede identificar.** Quien busca el domingo puede estar
   planificando un viaje del viernes. Si las búsquedas adelantan a los viajes *k* días, el perfil
   rotado *k* posiciones debería encajar con el de viajes. Ninguna rotación lo hace: el mejor ajuste
   es r = 0,34 contra las reglas y r = 0,60 contra la oferta, y con siete puntos hace falta
   |r| > 0,75 para hablar de correlación al 5 %.
2. **La amplitud no cuadra.** El perfil de búsquedas varía 1,20× a lo largo de la semana, frente a
   1,44× en las reglas y 1,54× en la oferta. Las búsquedas están mucho más repartidas que los
   viajes, que es justo lo que se espera si la gente planifica varios días antes.

De hecho, **domingo alto y viernes bajo es exactamente lo que predice la hipótesis del desfase**
—planificar el domingo, viajar el viernes—, así que el dato no contradice a las reglas: es compatible
con ellas y con su contrario, y por eso no decide.

**Una precaución metodológica.** El contraste de cada día **no se puede anclar en un
día concreto**. Al principio se medía la significación contra el martes y salía que ningún día se
distingue de nada: resulta que un pico de noticias del 20 de enero de 2026 cayó en martes, infló su
error y contaminó las siete comparaciones. Comparando cada día contra los otros seis juntos aparece el
patrón. El martes en 1,000 de la tabla es solo un cambio de unidad para poder ponerla al lado de las
reglas: dividir por otro día deja idénticas todas las proporciones, y el nivel absoluto no lo usa
nadie porque el reparto se reescala a media 1,0 antes de aplicar el total de CNMC.

**Por qué no entra en el pipeline.** Es un endpoint no documentado, devuelve una **muestra aleatoria**
—dos peticiones idénticas dan números distintos— y redondea a entero, así que una tabla construida con
esto no sería reproducible y rompería la trazabilidad del lago. Queda como experimento relanzable
(`make contraste-trends`), no como fuente.

### Qué coeficientes saldrían partiendo de cero, y el viernes revisado

Juntando lo medido se puede estimar el perfil semanal sin partir de ninguna hipótesis. La idea: **la
oferta es una señal revelada de la demanda** —el operador pone capacidad donde espera gente, y sabe
más que cualquier regla escrita a mano— pero no la sigue perfectamente, porque no puede partir trenes y prefiere ir más
lleno un día punta que añadir servicio. Así que la demanda es la oferta atenuada hacia la media:

```
demanda_i = oferta_i ^ alfa        con 0 <= alfa <= 1
```

Un único parámetro libre, y se acota **por los dos lados con dato medido**:

| cota | de dónde sale | resultado |
| --- | --- | --- |
| inferior | amplitud semanal del interés de búsqueda (1,20×) frente a la de la oferta (1,54×) | α ≥ 0,42 |
| inferior | techo del 100 %: por debajo, el sábado medio sale al 105-113 %, imposible | α ≥ 0,43 |
| superior | α = 1 es el caso límite en que el operador ajusta la capacidad a la demanda | α ≤ 1 |

**Las dos cotas inferiores son independientes y coinciden.** Una sale de cuánta gente busca y la otra
de cuántos asientos hay. La de Trends vale aunque el desfase entre buscar y viajar sea desconocido,
porque **rotar un perfil no cambia su amplitud**, y es una cota inferior porque las búsquedas de un
viaje se reparten entre varios días previos y esa convolución solo puede aplanar.

| día | α = 0,43 | α = 1,0 | oferta medida | en el fichero | (Trends, **no comparable**) |
| --- | --- | --- | --- | --- | --- |
| lun | 0,99 | 0,99 | 0,99 | 1,05 | 1,06 |
| mar | 1,00 | 1,00 | 1,00 | 1,00 | 1,00 |
| mié | 1,01 | 1,03 | 1,03 | 1,00 | 0,97 |
| jue | 1,02 | 1,04 | 1,04 | 1,05 | 0,99 |
| **vie** | 0,98 | 0,96 | 0,96 | **1,15** | 0,94 |
| sáb | 0,85 | 0,68 | 0,68 | 0,80 | 0,93 |
| dom | 0,89 | 0,76 | 0,76 | 1,00 | 1,11 |

Dos avisos sobre cómo leer esa tabla:

- **Las columnas α no son medidas independientes de la oferta**: son la oferta elevada a α, o sea la
  misma señal comprimida hacia la media. Hay **un** perfil medido, no tres.
- **La última columna está por referencia y no se puede usar día a día.** Los valores de Trends
  dependen del desfase entre buscar y viajar, que no se identifica (ver la sección anterior). De
  Trends solo sirve la amplitud del conjunto, que es lo que acota α.

**Se intentó bajar el viernes de 1,15 a 0,98 y se devolvió.** Queda escrito porque el intento
delimita lo que se puede medir, y porque el 1,15 sigue siendo el coeficiente con menos apoyo.

Lo que se midió, y es lo único que hay: el operador pone **65 trenes un viernes** (mediana) frente a
**71 un jueves**, y ha llegado a poner 78 un jueves. El viernes **no está topado por capacidad**:
pone menos porque quiere. Si fuera el día de más demanda de la semana, estaría dejando dinero en la
mesa.

Por qué no basta:

- **Es una sola medida, no tres.** La familia α da 0,96-0,98 para el viernes, pero **no es
  independiente de la oferta**: es la misma oferta comprimida hacia la media. Presentarlas como dos
  confirmaciones fue un error de razonamiento.
- **El 0,94 del interés de búsqueda no es utilizable, y esto está dicho tres párrafos más arriba en
  este mismo documento.** De Trends solo sirve la *amplitud*, porque no cambia al rotar el perfil;
  los valores por día dependen del desfase entre buscar y viajar, que no se identifica. Usarlo es
  exactamente el error que la sección anterior advierte.
- **La oferta arrastra el aviso de siempre:** el NAP da trenes, no plazas. Esos 65 trenes del viernes
  pueden ser composiciones más largas. Es el mismo problema que impide decidir el fin de semana, y
  vale igual aquí.

Y hay un motivo de coherencia: el argumento que sostiene el 1,15 —la demanda del viernes supera lo
que el operador ofrece y la diferencia se absorbe en ocupación, con un 85,7 % implicado, que es alto
pero posible— es **el mismo** que mantiene el domingo en 1,00. Aplicarlo a uno y no al otro no se
sostiene.

**Qué haría falta para cerrarlo:** plazas por día en vez de trenes, o demanda diaria observada.
Mientras no haya una de las dos, el viernes se queda como hipótesis documentada, igual que los
festivos.

**El sábado y el domingo, por la misma razón y una propia.** La ocupación implícita del fin de semana
está contaminada por el denominador (21 de los 25 días imposibles son fin de semana, ver arriba), así
que tampoco sirve de contraste. La familia α los pondría entre 0,68 y 0,89, pero el argumento de las
reglas sigue en pie y los datos disponibles no lo refutan.

**Resumen de lo que se puede y no se puede calibrar hoy:**

| coeficiente | ¿medido? | con qué |
| --- | --- | --- |
| `base_mes` | **sí** | turismo mensual del INE |
| `base_dia_semana`, lun a jue | **sí** | oferta del NAP; ocupación implícita coherente (73-84 %) |
| `base_dia_semana`, vie, sáb, dom | **no** | la oferta no distingue demanda alta de capacidad topada, y da trenes y no plazas |
| `festivo_entre_semana`, `puente`, vísperas | **no** | confundidos con la estacionalidad trimestral |
| factores de eventos | **no** | 63 días en un corredor y no existe el contrafactual |

**Los factores de los eventos tampoco.** Los que devuelve el LLM (1,15 para una feria, 1,25 para un
Clásico) no son estimables: son 63 días con evento en un solo corredor y no existe el escenario
contrafactual, es decir cuánta gente habría viajado ese día sin el evento.

**Conclusión:** `festivo_entre_semana`, `puente` y los ajustes de víspera y regreso **se mantienen
como hipótesis documentadas**, no porque no se haya intentado estimarlos, sino porque las fuentes
disponibles no contienen la información necesaria. `base_dia_semana` y `base_mes` sí salen de dato
medido, y queda escrito en el propio fichero de reglas de dónde sale cada uno.

De los tres intentos de contraste, uno funcionó (el mes, con el turismo del INE) y dos no (los
festivos con los trimestres de CNMC, y el día de la semana con el interés de búsqueda). Los dos
fracasos tienen la misma forma: **la fuente contiene la señal mezclada con otra que no se puede
separar** —la estacionalidad trimestral en un caso, el desfase entre planificar y viajar en el otro—.
Y el éxito también: funcionó porque medir la cuota dentro del trimestre descuenta precisamente lo que
estorbaba.

## Limitaciones

**La principal, y la que lo explica todo:** no existe ocupación diaria observada. Mientras el
objetivo se construya a partir de un reparto por reglas, ninguna variable puede aportar información
que no estuviera ya en esas reglas. El cuello de botella es de datos, no de modelado.

El perfil mensual no es una excepción a eso, es una confirmación: mejoró el resultado **metiendo el
dato en las reglas**, no en el modelo. El camino que queda abierto es el mismo —seguir sustituyendo
hipótesis del reparto por fuentes medidas—, no probar más algoritmos.

**El horizonte.** El plan pide predecir a 5 días. La meteo que entra es la **observada**, que a cinco
días no se conoce: habría que usar la previsión. Pero AEMET no archiva previsiones pasadas, así que
entrenar con ellas es imposible hacia atrás. Dado que la meteo no aporta nada (ver arriba), el punto
es discutible en la práctica, pero conviene decirlo.

**El tamaño.** 157 días utilizables y unos 26 por tramo de evaluación. Las diferencias de uno o dos
puntos de MAPE entre modelos no son concluyentes con esta muestra.

**La oferta no cubre el corredor completo.** Iryo no publica horarios en el NAP, así que las plazas
del denominador son las de Renfe y OUIGO. Y desde el 20 de agosto de 2026 el conjunto de OUIGO en el
NAP sirve, por error del proveedor, el horario ferroviario europeo en formato EDIFACT en lugar de su
GTFS.

**Qué cambiaría el resultado.** Solo una cosa: ocupación diaria observada, o cualquier medida real
de demanda diaria. Con eso, las mismas variables y el mismo código darían un experimento con
sentido. Sin eso, el modelo demuestra que el flujo funciona, no que la predicción acierte.
