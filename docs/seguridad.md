# Seguridad y control de acceso: el diseño

Esta parte de OB5 está **diseñada y no implementada**, y conviene decir por qué antes de entrar en el
detalle: el proyecto corre en un `docker compose` en un portátil, con todos los componentes hablando
por la red interna de Docker. Cifrar el tráfico entre dos contenedores de esa red no protege de nada
real; implementarlo sería teatro de seguridad y añadiría complejidad sin reducir ningún riesgo. Lo que
sí tiene valor es dejar escrito **qué habría que hacer si esto se desplegara de verdad**, y eso es lo
que hay aquí.

El diccionario de datos y el linaje, que son la otra mitad de OB5, sí están hechos:
[`gobierno-datos.md`](gobierno-datos.md).

- [Qué hay que proteger, de verdad](#qué-hay-que-proteger-de-verdad)
- [Control de acceso por roles](#control-de-acceso-por-roles)
- [Cifrado](#cifrado)
- [Credenciales](#credenciales)
- [Qué está hecho hoy](#qué-está-hecho-hoy)

---

## Qué hay que proteger, de verdad

El análisis de riesgo empieza por una pregunta que suele saltarse: **¿qué pasa si estos datos se
filtran?**

En este proyecto, **casi nada**, y por una razón concreta: **todas las fuentes son datos abiertos
publicados**. CNMC, INE, AEMET, BOE y el NAP publican su información para que terceros la usen; de
hecho dos de sus licencias **obligan** a redistribuir las obras derivadas. No hay datos personales, no
hay información comercial reservada y no hay nada bajo secreto estadístico (las filas que el INE marca
como tal se descartan en la ingesta).

Eso cambia dónde hay que poner el esfuerzo:

| riesgo | gravedad aquí | por qué |
| --- | --- | --- |
| **Confidencialidad** de los datos | **baja** | son públicos; cualquiera puede descargarlos de la fuente |
| **Confidencialidad de las credenciales** | **alta** | `AEMET_API_KEY` y `NAP_API_KEY` son personales y su uso se atribuye a quien las pidió |
| **Integridad** | **alta** | una tabla alterada sin que nadie lo note lleva a decisiones equivocadas, y la trazabilidad pierde su valor |
| **Disponibilidad** | media | si el lago se cae, se reconstruye desde las fuentes; lo que no se recupera son los snapshots diarios del NAP, que no tienen histórico |

**Conclusión del análisis:** el objetivo no es esconder los datos, es **proteger las credenciales y
garantizar que nadie altere el dato sin dejar rastro**. Un diseño que se centrara en cifrar las tablas
estaría resolviendo el problema menos grave de los cuatro.

---

## Control de acceso por roles

Hoy todo corre con un **único usuario administrador**, que lo puede todo. El diseño propuesto son tres
roles, que es lo mínimo para separar quién mira de quién cambia:

| rol | Superset | lago (MinIO) | para quién |
| --- | --- | --- | --- |
| **administrador** | todo, incluida la importación de dashboards | lectura y escritura en los tres buckets | quien mantiene la plataforma |
| **analista** | ver todos los dashboards, crear gráficas y usar SQL Lab sobre Gold | **solo lectura** de Gold | quien explota los datos |
| **consulta** | ver los dashboards publicados; sin SQL Lab | sin acceso directo | el resto del equipo, el tribunal |

**Tres reglas que dan forma al diseño:**

1. **Nadie escribe en Gold desde la interfaz.** Gold lo construyen los procesos (`make 05_gold`, la
   predicción), nunca una persona. Si un analista pudiera escribir, la trazabilidad dejaría de
   responder a «¿de qué ejecución salió esta cifra?».
2. **El acceso a Silver y Bronze no pasa por Superset.** Superset solo ve Gold. Quien necesite Silver
   trabaja con el código, que deja carga registrada.
3. **El rol de consulta no tiene SQL Lab.** No por ocultar nada —los datos son públicos— sino porque
   una consulta mal hecha sobre los Parquet de MinIO puede tumbar el servicio, y porque las cifras
   deben leerse con el contexto que llevan los dashboards.

Superset implementa esto de serie: roles con permisos por *dataset* y por *dashboard*. Serían un par
de horas de configuración, y la decisión del equipo ha sido **dejarlo documentado** porque el
entregable corre en local y con un solo usuario.

---

## Cifrado

### En tránsito

| tramo | hoy | en un despliegue real |
| --- | --- | --- |
| navegador → Superset | HTTP en `localhost:8088` | **HTTPS** con certificado, terminando en un proxy inverso delante |
| Superset → MinIO | HTTP en la red de Docker | HTTPS con certificado interno |
| procesos → Postgres | sin TLS | `sslmode=require` |
| ingesta → fuentes externas | **ya es HTTPS** | igual |

Lo único que hoy sale de la máquina es la ingesta, y **ya va cifrada**: todas las URL del registro son
`https://`. Lo demás no cruza ninguna red.

### En reposo

- **MinIO** admite cifrado del lado del servidor (SSE-S3 o SSE-KMS) activable por *bucket*. Es la
  pieza que guarda Bronze, Silver y Gold.
- **Postgres** no cifra por sí mismo; lo habitual es cifrar el disco o el volumen.
- En la práctica, en un despliegue en la nube esto se resuelve con el cifrado en reposo que el
  proveedor ofrece por defecto.

**Dónde sí importa aquí y no es teatro:** el fichero `.env`, que está en el disco en claro y tiene las
claves de AEMET y del NAP y las credenciales de MinIO y Postgres.

---

## Credenciales

Esta es la parte donde el proyecto **sí toma decisiones hoy**, y conviene listarlas porque son las que
de verdad reducen riesgo:

- **Ninguna credencial está en el repositorio.** El registro de fuentes declara el **nombre de la
  variable de entorno**, nunca el valor:
  ```yaml
  auth: {env: NAP_API_KEY, header: ApiKey}
  ```
  Si falta la variable, la descarga falla con un mensaje que dice cuál es y dónde pedirla.
- **El `.env` está fuera de git** y hay un `.env.example` con las claves vacías.
- **Las credenciales no viajan a los logs.** La trazabilidad guarda el destino y los parámetros de
  cada carga, no las cabeceras de la petición.

**Lo que haría falta en producción:** un gestor de secretos (Vault, AWS Secrets Manager, el de la nube
que sea) en vez de un fichero, y rotación periódica de las claves de API.

---

## Qué está hecho hoy

Para que la distinción entre diseñado e implementado quede clara:

| | estado |
| --- | --- |
| Diccionario de datos y linaje (OB5) | **implementado** — [`gobierno-datos.md`](gobierno-datos.md) |
| Licencias y atribución (OB5) | **implementado** — `LICENSE`, `NOTICE` y el pie de los dashboards |
| Credenciales fuera del repositorio | **implementado** |
| Ingesta por HTTPS | **implementado** (es lo único que sale de la máquina) |
| Trazabilidad de quién escribió qué y cuándo | **implementado** — cada carga registra proceso, usuario y ejecutor |
| Control de acceso por roles | **diseñado** — aquí |
| Cifrado en reposo y en tránsito interno | **diseñado** — aquí |
| Gestor de secretos | **diseñado** — aquí |

**La integridad, que es el riesgo alto, sí está atendida** aunque no se hable de seguridad al
describirla: los quality gates rechazan lo que no cumple el contrato y lo mandan a cuarentena en vez
de dejarlo entrar, y cada escritura en el lago deja una fila con su `run_id`, su proceso y su usuario.
Preguntar «¿de dónde salió esta cifra y qué controles pasó?» tiene respuesta.
