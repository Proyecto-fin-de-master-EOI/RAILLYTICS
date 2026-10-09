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
`plausible = False` porque dan una ocupación por encima del 100 %, lo que señala días en que el
conteo de trenes del NAP falla. No se borran, se marcan.

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
variable `mes` predice mejor porque metimos un perfil mensual en las reglas que generan el objetivo,
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

**Los factores de los eventos tampoco.** Los que devuelve el LLM (1,15 para una feria, 1,25 para un
Clásico) no son estimables: son 63 días con evento en un solo corredor y no existe el escenario
contrafactual, es decir cuánta gente habría viajado ese día sin el evento.

**Conclusión:** `festivo_entre_semana`, `puente` y los ajustes de víspera y regreso **se mantienen
como hipótesis documentadas**, no porque no se haya intentado estimarlos, sino porque las fuentes
disponibles no contienen la información necesaria. `base_dia_semana` y `base_mes` sí salen de dato
medido, y queda escrito en el propio fichero de reglas de dónde sale cada uno.

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
