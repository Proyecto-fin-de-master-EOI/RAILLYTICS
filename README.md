# RAILLYTICS

**Raillytics Light — Plataforma de Ingeniería de Datos para Análisis y Predicción de Demanda Ferroviaria en España**

Proyecto de TFM (Máster en Ingeniería de Datos — Grupo 3). Plataforma end-to-end que integra, procesa y analiza datos ferroviarios públicos (Renfe Open Data, AEMET, festivos BOE, INE) para generar insights operativos y predicciones de demanda a 30 días.

**Stack tecnológico:** Python (ingesta) · PySpark (procesamiento) · Delta Lake + Parquet (almacenamiento) · dbt (modelo dimensional) · Snowflake (Data Warehouse) · Scikit-learn (modelo predictivo) · Power BI (dashboards).

**Arquitectura:** patrón Medallion — Bronze (datos brutos) → Silver (datos limpios y enriquecidos) → Gold (modelo dimensional listo para consumo analítico).

---

## Equipo

- Elena Calcerrada Quiles
- Joaquín Ayllón Moreno
- Laura Rodríguez Mora
- Miguel Ángel Vicente Vicente
- Rubén Martínez Sierra

---

## Arquitectura de datos (Medallion)

Los datos fluyen por tres capas de calidad creciente:

```
Fuentes (Renfe, AEMET, BOE, INE)
        │  ingesta Python
        ▼
🥉 BRONZE ──► 🥈 SILVER ──► 🥇 GOLD ──► Snowflake ──► Power BI
   datos        PySpark        dbt
   en bruto     limpieza      modelo
                              dimensional
```

- **🥉 Bronze — datos en bruto**: los scripts Python de ingesta descargan las fuentes públicas y las guardan tal cual llegan, sin transformar, en formato Parquet/Delta Lake particionado por fecha y fuente. Si algo falla después, siempre se puede volver al dato original.
- **🥈 Silver — datos limpios y enriquecidos**: jobs PySpark eliminan duplicados, tratan nulos, normalizan formatos (fechas, nombres de estaciones) y cruzan los viajeros con meteorología y festivos. Es la capa de "datos fiables".
- **🥇 Gold — datos listos para el análisis**: dbt construye el modelo dimensional (dimensiones `Dim_Estacion`, `Dim_Linea`, `Dim_Fecha` y hechos `Fact_Viajeros`, `Fact_Puntualidad`) y lo carga en Snowflake, desde donde consumen Power BI y el modelo predictivo de Scikit-learn.

---

## Estructura de directorios

```
RAILLYTICS/
├── README.md
├── G3.pdf                      # Documento de diseño del proyecto
├── requirements.txt            # Dependencias Python del proyecto
├── .env.example                # Plantilla de variables de entorno (API keys, credenciales)
│
├── config/                     # Configuración (rutas, parámetros de conexión, scheduling)
│
├── data/                       # Data Lake local — NO se versiona en git
│   ├── bronze/                 # Datos en bruto (Parquet/Delta particionado por fecha y fuente)
│   │   ├── renfe/
│   │   ├── aemet/
│   │   ├── festivos/
│   │   └── ine/
│   ├── silver/                 # Datos limpios, normalizados y enriquecidos (Delta Lake)
│   └── gold/                   # Modelo dimensional local previo a carga en Snowflake
│
├── python/
│   └── raillytics/
│       ├── ingesta/            # Scripts Python de descarga y validación (capa Bronze)
│       ├── procesamiento/      # Jobs PySpark de limpieza y enriquecimiento (capa Silver)
│       ├── ml/                 # Modelo predictivo Scikit-learn (features, entrenamiento, evaluación)
│       └── utils/              # Utilidades comunes (logging, validación de esquemas, helpers)
│
├── build.sbt                   # Proyecto SBT (Scala 2.13 / Spark 4.2) en la raíz para que IntelliJ lo reconozca
├── project/                    # Metadatos del build SBT (build.properties)
├── src/
│   ├── main/scala/raillytics/  # Jobs Spark en Scala
│   └── test/scala/raillytics/  # Tests ScalaTest
│
├── dbt/                        # Proyecto dbt: transformaciones Silver → Gold
│   ├── models/
│   │   ├── staging/
│   │   └── marts/              # Dimensiones (Dim_Estacion, Dim_Linea, Dim_Fecha) y hechos (Fact_Viajeros, Fact_Puntualidad)
│   └── tests/                  # Tests de calidad dbt (unicidad, integridad referencial, rangos)
│
├── notebooks/                  # Notebooks de exploración y análisis (EDA, validación de fuentes)
│
├── dashboards/                 # Ficheros Power BI (.pbix) y documentación de los 4 dashboards
│
├── tests/                      # Tests unitarios de ingesta y transformaciones PySpark
│
└── docs/                       # Documentación técnica y memoria del TFM
```

---

## Criterios de nombrado de ramas

| Rama         | Propósito                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------- |
| `main`       | Rama estable. Solo recibe merges desde `develop` vía Pull Request. Nunca se hace commit directo.   |
| `develop`    | Rama de integración. Todo el trabajo diario se fusiona aquí mediante PRs.                          |
| `feat_*`     | Nueva funcionalidad. Ej.: `feat_ingesta_renfe`, `feat_modelo_demanda`.                             |
| `fix_*`      | Corrección de errores. Ej.: `fix_nulos_aemet`.                                                     |
| `docs_*`     | Cambios de documentación. Ej.: `docs_memoria_tfm`.                                                 |
| `refactor_*` | Mejoras de código sin cambio funcional. Ej.: `refactor_jobs_silver`.                               |

Reglas de nombrado:

- Todo en **minúsculas**, palabras separadas por guion bajo (`_`).
- Nombre descriptivo y corto, en castellano: `feat_dashboard_puntualidad`, no `feat_cosas`.
- Una rama = un objetivo. Si la tarea crece, se divide en varias ramas.
- Las ramas parten siempre de `develop` actualizado (`git pull` antes de crearla).

---

## Modo de trabajo con Pull Requests

1. **Crear la rama** desde `develop`:
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feat_nombre_descriptivo
   ```
2. **Desarrollar y commitear** con mensajes claros en castellano, en imperativo y con prefijo del tipo de cambio: `feat: añade ingesta de festivos BOE`, `fix: corrige duplicados en viajeros`.
3. **Publicar la rama y abrir la PR** contra `develop`:
   ```bash
   git push -u origin feat_nombre_descriptivo
   ```
   La PR debe incluir: descripción del cambio, motivación y cómo probarlo.
4. **Revisión obligatoria**: al menos **1 aprobación** de otro miembro del equipo antes de fusionar. El autor no aprueba su propia PR.
5. **Merge a `develop`**: preferiblemente con *squash merge* para mantener el historial limpio. La rama se elimina tras el merge.
6. **Merge a `main`**: solo desde `develop`, al cerrar un hito (fin de capa Bronze, Silver, Gold, modelo, dashboards…), mediante PR revisada por el equipo.

Normas generales:

- Nunca hacer `push` directo a `main` ni a `develop`.
- PRs pequeñas y frecuentes: más fáciles de revisar que una PR gigante.
- Resolver los conflictos en la rama de la feature (rebase o merge de `develop` hacia la rama), nunca en `develop`.
