# Hipótesis del Sprint 2: qué se afirmó, cómo se midió y qué salió

El plan del sprint fija tres hipótesis con un umbral numérico cada una. Aquí está el veredicto de las
tres, con el procedimiento y la cifra, para que cualquiera pueda rehacerlas. Al final hay una cuarta
(H5) que no venía en el plan: se contrastó al construir el precio recomendado y hace falta para
entender de dónde sale el único coeficiente declarado de esa regla.

| | hipótesis | umbral | resultado | |
| --- | --- | --- | --- | --- |
| **H2** | el error de recalibración de la DTC es despreciable | < 0,001 | **0,0** | **se cumple** |
| **H3** | las variables externas mejoran la predicción | ≥ 1,5 pp | **−0,76 pp** | **no se cumple** |
| **H4** | la carga completa cabe en una ventana operativa | < 4 h | **31,6 min** | **se cumple** |
| **H5** | la elasticidad precio se puede estimar con la CNMC | significativa | **t = −0,66** | **no se cumple** |

Dos de tres. Y la que falla es la más interesante, porque falla **por una razón estructural que se
puede explicar**, no porque algo esté mal programado. Lo mismo le pasa a H5.

---

## H2 — el reparto diario no pierde viajeros

**Qué afirma.** La DTC coge el total trimestral de la CNMC y lo reparte entre los días. Si al sumar
los días no sale el total, el reparto está inventando o perdiendo viajeros.

**Cómo se mide.** Sumar `viajeros_previstos` de todos los días de una ejecución y compararlo con el
`total_esperado` de esa misma ejecución. Está en `gold/fact_prediccion_demanda`, así que se comprueba
con una consulta:

```sql
SELECT run_id, max(total_esperado) AS objetivo, sum(viajeros_previstos) AS repartido
FROM fact_prediccion_demanda GROUP BY run_id
```

**Resultado: error exactamente 0** en las tres ejecuciones de 2026-T4 (92 días cada una).

**Por qué sale 0 y no «casi 0»**, que es lo que el umbral esperaba: el reparto no se hace con números
en coma flotante. Cada día recibe su cuota como **fracción exacta** (`fractions.Fraction`) y los
viajeros que sobran al redondear se asignan por el **método del mayor resto** —de uno en uno, a los
días con mayor parte decimal—. Así la suma es el total por construcción, no por aproximación.

El umbral de 0,001 daba por hecho que habría error de redondeo. No lo hay. Esa decisión está en
`python/raillytics/prediccion/normalizar.py` y hay un test que la vigila.

---

## H3 — las variables externas no mejoran la predicción

**Qué afirma.** Que añadir festivos, eventos y meteorología mejora el error del modelo en al menos
1,5 puntos porcentuales frente a no usarlas.

**Cómo se mide.** Un A/B por ablación: se entrena el modelo quitando cada grupo de variables y se mira
cuánto empeora el MAPE medio de la validación temporal en cinco tramos. Si una variable aporta,
quitarla tiene que empeorar el resultado.

**Resultado:**

```
con todas las variables      10,1 %   (referencia trivial 10,3 %)
solo el día de la semana     10,8 %
```

| grupo | columnas | MAPE sin él | cambio | |
| --- | --- | --- | --- | --- |
| mes | 1 | 10,4 % | **+0,3 pp** | aporta algo |
| día de la semana | 7 | 10,1 % | +0,0 pp | — |
| **festivos** | 3 | 10,1 % | **+0,0 pp** | **no aporta** |
| **vísperas y puentes** | 3 | 10,1 % | **+0,0 pp** | **no aporta** |
| **eventos** | 7 | 10,1 % | **+0,0 pp** | **no aporta** |
| **meteo** | 4 | 9,3 % | **−0,8 pp** | **estorba** |

**Y el A/B con la definición literal del plan** —modelo base con solo las variables de calendario
frente al enriquecido con festivos, vísperas, eventos y meteo, medido con validación temporal—:

```
solo calendario   9,3 %
enriquecido      10,1 %
mejora          −0,76 pp     (el plan pide >= +1,5)
```

**Veredicto: la hipótesis no se cumple.** No es que las variables externas no aporten: **empeoran**
el modelo en 0,76 puntos. La ablación por grupos dice lo mismo desde otro ángulo: ninguna llega a
1,5 pp, la mayoría da exactamente cero y la meteorología resta.

El número se publica en `fact_metricas_modelo` con cada entrenamiento, así que no hay que rehacer el
experimento para consultarlo.

**Por qué, y está medido.** No es un fallo de implementación: es consecuencia de cómo se construye el
objetivo. La ocupación es `demanda ÷ oferta`. La demanda la reparten reglas que miran el calendario, y
la oferta solo varía por día de la semana —está medido que es plana en festivos y en días con evento,
factor 1,02—. Al dividir una entre otra, **los festivos se cancelan**: suben el numerador y bajan el
denominador a la vez. Lo único que sobrevive es el patrón semanal, que es precisamente el que
introducen las propias reglas de reparto.

Los eventos no aportan por otra razón añadida: son 63 días de 400 y en ellos la oferta no se mueve.

**Lo que haría que la hipótesis fuera comprobable de verdad:** ocupación diaria observada, o cualquier
medida real de demanda diaria. Mientras el objetivo salga de un reparto por reglas, ninguna variable
puede aportar información que no estuviera ya en esas reglas. El detalle completo está en
[`modelo-ocupacion.md`](modelo-ocupacion.md).

**Una trampa que conviene nombrar**, porque es fácil caer en ella al evaluar esta hipótesis: el MAPE
del modelo **no sirve** para decidir si una variable o un coeficiente son correctos. El objetivo lo
generan las propias reglas, así que tocarlas cambia el MAPE sin que eso diga nada sobre el mundo. El
código avisa cuando el MAPE baja del 5 % por ese motivo.

---

## H4 — la carga completa cabe de sobra en la ventana

**Qué afirma.** Que procesar todo el recorrido —descarga, Bronze, Silver, Gold— cabe en menos de
cuatro horas, que es lo que hace viable una ejecución diaria.

**Cómo se mide.** `make carga-e2e`, que encadena las fases y cronometra cada una.

**Resultado: 31,6 minutos**, con las 13 fuentes dentro:

| fase | tiempo |
| --- | --- |
| descarga | 248 s |
| Bronze l1 | 227 s |
| Bronze l2 (Parquet) | 403 s |
| Silver | 437 s |
| Gold | 584 s |
| **total** | **31,6 min** |

**Se cumple con mucho margen**: un 13 % del presupuesto.

**Dos avisos para no leer el número de más:** no incluye las cargas iniciales —el histórico del NAP
son unos 20 minutos más y se hace una vez, no cada día— y se midió en una máquina de desarrollo, no en
un servidor.

---

## H5 — la elasticidad precio de la demanda se puede estimar con los datos de la CNMC

**No se cumple**, y el motivo es el mismo de H3: la fuente no contiene la variación que haría falta.
Queda escrito porque de aquí sale el precio recomendado del Sprint 3, y conviene que se vea de dónde
viene el único número declarado de esa regla.

Lo que se hizo: un panel de 45 observaciones (3 operadores × 15 trimestres) del corredor
Madrid–Barcelona, cruzando `precio_medio_eur` de `silver/cnmc_precio_trimestral` con los viajeros de
`silver/cnmc_trimestral`. Regresión de log(viajeros) sobre log(precio) con efectos fijos de operador,
trimestre del año y tendencia lineal.

| término | coeficiente | error estándar | t |
| --- | --- | --- | --- |
| **log(precio)** | **−0,299** | **0,453** | **−0,66** |
| tendencia | +0,015 | 0,016 | 0,95 |
| operador = RENFE | +1,180 | 0,202 | 5,83 |
| operador = OUIGO | −0,167 | 0,125 | −1,34 |

R² 0,78 con 37 grados de libertad. El signo es el correcto —más precio, menos viajeros— pero no es
significativo ni de lejos: el intervalo de confianza al 95 % va de −1,22 a +0,62, o sea que incluye el
cero y también la posibilidad de que subir el precio aumente la demanda.

**Las dos razones de fondo**, que no se arreglan con más trimestres:

1. **Endogeneidad.** El operador sube el precio precisamente cuando prevé demanda alta. Precio y
   viajeros se mueven juntos por una causa común, así que el coeficiente estimado mezcla la respuesta
   del viajero al precio con la respuesta del operador a la demanda, y se va hacia cero.
2. **El corredor está topado por capacidad.** Entre 2023-T1 y 2025-T4 el precio medio sube un 72 %
   (42,87 → 73,61 €) y los viajeros se quedan entre 3,3 y 3,9 millones por trimestre. Esa serie no
   mide cuánta demanda hay: mide **cuántas plazas se vendieron**. No se puede estimar una curva de
   demanda con la oferta tocando el techo.

**Cómo se comprobaría:** hace falta una fuente de variación del precio ajena a la demanda —un cambio
de canon, una huelga del competidor, una entrada o salida de operador— y precio y demanda a la misma
frecuencia. Con eso se identificaría por variables instrumentales.

**Qué se hizo en su lugar:** declarar la elasticidad en `config/reglas_demanda.yml` con el valor
−1,00, que está dentro del intervalo de confianza de la estimación propia y es el valor neutro (el
exponente vale 1 y la regla se queda en «un 10 % de ocupación por encima de la referencia, un 10 % de
precio por encima»). No se toma prestada una cifra de la literatura: lo publicado para este corredor
es anterior a la liberalización, de un mercado con un solo operador ferroviario, y copiar el número
daría una falsa precisión. El precio base y el rango al que se acota la recomendación **sí son dato
medido** de la CNMC.

---

## El ratio de aceptación: el único KPI del plan que no se puede suplir

El plan pide, junto al precio recomendado, un **ratio de aceptación**: qué parte de las
recomendaciones adopta sin modificar el revenue manager. No está en el cuadro de mandos y no va a
estarlo, y la razón merece quedar separada de las demás.

El proyecto presupone datos que no tiene en varios sitios, y está bien justificado: el objetivo es una
arquitectura de ingeniería de datos robusta, no un dato perfecto. Pero en todos esos sitios lo que se
presupone es una **entrada** —una demanda diaria, un perfil por franja, un coeficiente—. El ratio de
aceptación sería distinto en naturaleza: fabricaría la **validación de nuestra propia salida**. El KPI
diría que el modelo acierta porque nosotros hemos escrito que acierta. Es el único sitio del proyecto
donde el supuesto se autoevalúa, y por eso no se simula.

Lo que sí se ha hecho son las dos cosas que quedan en pie:

- **Montar el mecanismo.** `gold/fact_precio_recomendado` guarda cada recomendación con sus entradas,
  la ocupación de referencia, la `version_regla` que la produjo y el `run_id` de la carga. El día que
  hubiera un operador, el ratio se calcula sobre esa tabla sin tocar nada. Sin la firma de la regla no
  sería atribuible: no se sabría qué se aceptó o se rechazó.
- **Publicar en su hueco algo medible que prueba el modelo y no al humano**: el porcentaje de
  recomendaciones que caen dentro del recorrido de precios que ese operador ha puesto de verdad en el
  corredor. Sale **86,2 %**, y las que se salen son casi todas de RENFE por abajo, porque su suelo
  observado (61,92 €) está cerca de su precio medio y la regla no tiene margen para proponer
  descuento.

---

## Qué dejan estas medidas

La parte de **ingeniería** responde: el reparto es exacto y el recorrido completo cabe de sobra en la
ventana. Las dos hipótesis que dependían del pipeline se cumplen.

La parte de **modelado** no, y señala siempre al mismo sitio. El cuello de botella no es el algoritmo,
ni las variables, ni el ajuste: es que **no existe demanda diaria observada** para este corredor.
Mientras el objetivo se construya repartiendo un total trimestral con reglas, el modelo solo puede
redescubrir esas reglas.

Eso tiene un corolario práctico que sí se ha podido aprovechar: **si la señal es la que metieron las
reglas, mejorar las reglas mejora el resultado**. Sustituir el perfil mensual inventado por el medido
con el turismo del INE llevó la validación temporal del 12,0 % al 10,1 %. No por modelar mejor, sino
por medir mejor.
