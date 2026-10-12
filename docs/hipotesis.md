# Hipótesis del Sprint 2: qué se afirmó, cómo se midió y qué salió

El plan del sprint fija tres hipótesis con un umbral numérico cada una. Aquí está el veredicto de las
tres, con el procedimiento y la cifra, para que cualquiera pueda rehacerlas.

| | hipótesis | umbral | resultado | |
| --- | --- | --- | --- | --- |
| **H2** | el error de recalibración de la DTC es despreciable | < 0,001 | **0,0** | **se cumple** |
| **H3** | las variables externas mejoran la predicción | ≥ 1,5 pp | **+0,0 pp** | **no se cumple** |
| **H4** | la carga completa cabe en una ventana operativa | < 4 h | **31,6 min** | **se cumple** |

Dos de tres. Y la que falla es la más interesante, porque falla **por una razón estructural que se
puede explicar**, no porque algo esté mal programado.

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

**Veredicto: la hipótesis no se cumple.** Ninguna variable externa llega a 1,5 pp; la mayoría da
exactamente cero y la meteorología **empeora** el modelo.

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

## Qué dejan estas tres medidas

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
