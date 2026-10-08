# Modelo de ocupación diaria: qué se hizo y qué salió

Experimento de modelado del coeficiente de ocupación diaria del corredor Madrid–Barcelona, que es
lo que pide el Sprint 2 («Silver enriquecido para un GBTRegressor que prediga el coeficiente de
ocupación a 5 días con MAPE ≤ 15 %»).

**Resumen:** el modelo cumple el umbral de MAPE, pero una validación temporal seria muestra que **no
mejora a una referencia trivial**, y ninguna de las variables externas aporta nada. No es un fallo
de implementación: es consecuencia de cómo está construido el objetivo, y se explica abajo con los
números.

- [Qué se predice](#qué-se-predice)
- [Cómo se construye el objetivo](#cómo-se-construye-el-objetivo)
- [Decisiones de diseño](#decisiones-de-diseño)
- [Resultados](#resultados)
- [Qué aporta cada variable](#qué-aporta-cada-variable)
- [Por qué sale así](#por-qué-sale-así)
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

Quedan **180 días** (2026-T1 y 2026-T2), de los que **157 son utilizables**: 23 se marcan
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

Con una sola partición (los últimos 32 días como evaluación):

```
MAPE 7.9 %  ·  error medio 6.7 puntos de ocupación  ·  R² 0.18
```

Cumple el umbral del plan. **Pero ese número es frágil**: depende de qué semanas tocaron. Con
validación temporal en cinco tramos, comparando siempre contra la referencia trivial —predecir la
media de lo ya visto—:

| tramo | días evaluados | MAPE | MAPE de la referencia | R² | ¿gana? |
| --- | --- | --- | --- | --- | --- |
| 1 | 29-ene a 24-feb | 12,1 % | 12,1 % | −0,10 | no |
| 2 | 25-feb a 23-mar | 13,8 % | 15,3 % | −0,74 | sí |
| 3 | 24-mar a 23-abr | 17,4 % | 9,9 % | −2,13 | no |
| 4 | 24-abr a 27-may | 8,5 % | 10,1 % | −0,41 | sí |
| 5 | 28-may a 30-jun | 8,0 % | 9,4 % | 0,25 | sí |
| **media** | | **12,0 %** | **11,4 %** | | **3 de 5** |

**El modelo es, en media, ligeramente peor que predecir siempre la media.** El 7,9 % de la partición
única era el mejor tramo de los cinco.

El tramo 3 es el peor con diferencia (17,4 % frente a 9,9 %): es **Semana Santa**. Las reglas mueven
mucho la demanda esos días y el modelo, entrenado solo con enero a marzo, no tiene con qué
anticiparlo.

## Qué aporta cada variable

Entrenando sin cada grupo y midiendo cuánto empeora el MAPE medio de los cinco tramos:

```
con todas las variables      12,0 %
solo el día de la semana     11,7 %      <- mejor con menos
```

| grupo | columnas | MAPE sin él | cambio | |
| --- | --- | --- | --- | --- |
| día de la semana | 7 | 12,3 % | +0,4 pp | **aporta** |
| mes | 1 | 11,8 % | −0,2 pp | da igual |
| festivos | 3 | 12,0 % | +0,0 pp | da igual |
| vísperas y puentes | 3 | 12,0 % | +0,0 pp | da igual |
| eventos | 7 | 12,0 % | +0,0 pp | da igual |
| meteo | 4 | 11,8 % | −0,2 pp | da igual |

**Ninguna variable externa aporta.** Ni los festivos del BOE, ni los eventos curados a mano, ni la
meteorología de AEMET. Un modelo con solo el día de la semana es ligeramente mejor que uno con las
26 variables.

La temperatura de Madrid aparecía con un 15 % en la importancia por permutación de una partición
única, pero al quitarla el modelo mejora: era ruido, no señal. Es un buen recordatorio de que la
importancia por permutación sobre un solo corte no demuestra nada.

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

## ¿Se pueden inferir los coeficientes en vez de ponerlos a mano?

Los pesos de `config/reglas_demanda.yml` son hipótesis. Dos de ellos se pudieron calibrar y el resto
no, y la diferencia tiene una causa clara: **hace falta una señal diaria**.

**El día de la semana sí.** Se calibró contra la oferta real del NAP, que da trenes por día. Es el
único dato diario medido que existe en el proyecto.

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
disponibles no contienen la información necesaria.

## Limitaciones

**La principal, y la que lo explica todo:** no existe ocupación diaria observada. Mientras el
objetivo se construya a partir de un reparto por reglas, ninguna variable puede aportar información
que no estuviera ya en esas reglas. El cuello de botella es de datos, no de modelado.

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
