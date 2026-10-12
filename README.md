# Operaciones de Aprendizaje Automático I — Trabajo Práctico Final

**Especialización en Inteligencia Artificial (CEIA) — FIUBA**

Implementación en un ambiente productivo simulado del modelo desarrollado en Aprendizaje de Máquina I, usando herramientas de MLOps: orquestación con **Apache Airflow**, ciclo de vida de modelos con **MLflow**, Data Lake compatible con **S3** y servicio del modelo mediante una **API REST** con FastAPI. Todo el sistema corre en contenedores **Docker**.

Proyecto basado en el template [amq2-service-ml](https://github.com/facundolucianna/amq2-service-ml) de la cátedra.

## Integrantes

* Luis Jose Paredes Ramirez 
* Diego Vazquez
* Hernando Scheidl 
* Nicolás Lastra 

---

## Arquitectura

Supuesto de la consigna: trabajamos para **ML Models and something more Inc.**, que ofrece modelos mediante una REST API y cuenta internamente con Airflow, MLflow y un Data Lake en S3.

```
                        ┌──────────────────────────┐
                        │   Apache Airflow (3.0)   │
                        │ scheduler · worker · api │──── cola de tareas ──► Valkey (Redis)
                        └────────────┬─────────────┘
                 lee/escribe datos   │   registra runs y modelos
                        ┌────────────┴─────────────┐
                        ▼                          ▼
          ┌──────────────────────┐     ┌──────────────────────┐
          │   RustFS (S3)        │◄────│   MLflow (3.16)      │
          │   s3://data          │     │   tracking + registry│
          │   s3://mlflow        │     └──────────┬───────────┘
          └──────────────────────┘                │ metadata
                        ▲                         ▼
                        │              ┌──────────────────────┐
                        │              │   PostgreSQL         │
                        │              │   airflow · mlflow_db│
                        │              └──────────────────────┘
          ┌─────────────┴────────┐
          │   FastAPI            │  carga el modelo champion desde MLflow
          │   REST API           │
          └──────────────────────┘
```

| Servicio | Rol | URL local | Credenciales |
|---|---|---|---|
| Apache Airflow | Orquestación de DAGs | http://localhost:8080 | `airflow` / `airflow` |
| MLflow | Tracking de experimentos y Model Registry | http://localhost:5001 | — |
| RustFS (consola) | Data Lake S3: buckets `data` y `mlflow` | http://localhost:9001 | `minio` / `minio123` |
| RustFS (API S3) | Endpoint para `boto3` / MLflow | http://localhost:9000 | `minio` / `minio123` |
| FastAPI | Servicio del modelo | http://localhost:8800/docs | — |
| PostgreSQL | Metadata de Airflow y MLflow | `localhost:5432` | `airflow` / `airflow` |
| Valkey (Redis) | Cola de Celery para Airflow (solo red interna) | — | — |

> Los puertos se configuran en el archivo `.env`.

### Cambios respecto al template de la cátedra

- **MinIO → RustFS.** En septiembre de 2026 MinIO retiró sus imágenes públicas de Docker Hub y quay.io, por lo que el servicio `s3` no podía descargarse. Se reemplazó por RustFS, compatible con la API de S3. El servicio mantiene el nombre `s3` y las mismas credenciales, así que el resto del sistema no cambia.
- **Creación de buckets con AWS CLI.** El contenedor `create_s3_buckets` usa la imagen oficial `amazon/aws-cli` en lugar del cliente `mc` de MinIO.
- **Driver de Postgres explícito en MLflow.** La conexión del servidor de MLflow usa `postgresql+psycopg2://`. Con las versiones actuales de SQLAlchemy, el driver por defecto pasó a ser `psycopg` (v3), que no está instalado.
- **Versiones fijadas.** Las librerías que guardan y cargan el modelo tienen la misma versión exacta en el entorno local y en todos los contenedores.
- **`MLFLOW_TRACKING_URI`** definido en Airflow y FastAPI, para no configurarlo en cada DAG.
- **La API no depende de Airflow.** El servicio `fastapi` solo espera a MLflow, y su healthcheck usa `/health`.
- **Virtualenv de ML en la imagen de Airflow** (`/opt/ml-venv`). Las tareas de ML corren ahí con `@task.external_python`, con las mismas versiones que la API y sin tocar las dependencias de Airflow.

---

## Estructura del repositorio

```
.
├── airflow/                 # Se monta en vivo en los contenedores de Airflow
│   ├── dags/                # DAGs del proyecto (solo definen el flujo)
│   │   ├── etl_tmdb_movies.py
│   │   ├── retrain_revenue_model.py
│   │   └── utils/           # Lógica de las tareas: config, s3, data, model
│   ├── secrets/             # variables.yaml y connections.yaml de Airflow
│   ├── plugins/
│   ├── config/              # Generado por Airflow (ignorado por git)
│   └── logs/                # Generado por Airflow (ignorado por git)
├── dockerfiles/             # Recetas de las imágenes de cada servicio
│   ├── airflow/             # apache/airflow:3.0.2 + venv de ML (requirements-ml.txt)
│   ├── fastapi/             # API REST: app.py (endpoints), model.py (modelo), schema.py
│   ├── mlflow/              # Servidor de MLflow
│   └── postgres/            # Postgres + creación de la base mlflow_db
├── notebooks/               # Experimentación local conectada a los servicios
├── tests/                   # Tests de utils y de la API (pytest)
├── docs/                    # Diagramas y documentación adicional
├── docker-compose.yaml      # Definición de todos los servicios
├── pyproject.toml           # Dependencias del entorno local (uv)
├── uv.lock                  # Versiones exactas del entorno local
└── .env                     # Puertos y credenciales (valores de ejemplo)
```

---

## Puesta en marcha

### 1. Levantar los servicios

```bash
git clone <url-del-repo>
cd <carpeta-del-repo>
docker compose --profile all up -d
```

La primera vez tarda varios minutos (descarga y construye las imágenes). Verificar el estado con:

```bash
docker compose --profile all ps
```

Todos los servicios deben quedar `healthy`. `create_s3_buckets` y `airflow-init` son tareas de inicialización: es normal que terminen con `Exited (0)`.


### 2. Entorno local (notebooks)

```bash
uv sync
uv run python -m ipykernel install --user --name ceia-mlops --display-name "ceia-mlops"
```

Abrir las notebooks con Jupyter y elegir el kernel `ceia-mlops`.

### 3. Verificar la conexión

1. Ejecutar `notebooks/00_prueba_conexion.ipynb`: sube un dataset de prueba a `s3://data/prueba/` y registra un experimento `prueba` en MLflow.
2. En Airflow, activar y ejecutar el DAG `prueba_s3`: lee ese archivo desde el worker y escribe `s3://data/prueba/resumen.csv`.

### 4. Pipeline de datos y reentrenamiento

En Airflow (http://localhost:8080) hay dos DAGs encadenados:

| DAG | Se ejecuta | Qué hace |
|---|---|---|
| `etl_tmdb_movies` | A mano | Descarga el dataset de TMDB de Kaggle, lo limpia, arma las features y lo separa en train/test en `s3://data/final/`. Registra la corrida en el experimento `tmdb_revenue_etl` de MLflow. |
| `retrain_revenue_model` | Solo, cuando termina el ETL (asset `s3://data/final/tmdb`) | Entrena un *challenger* con los hiperparámetros del *champion*, compara el MAE de los dos sobre el mismo set de testeo y promueve al challenger si es mejor. El modelo es `revenue_regressor` en el Model Registry; la API usa el alias `champion`. |

Para usarlos, activar los dos DAGs y ejecutar `etl_tmdb_movies` (el porcentaje de testeo se puede cambiar con el parámetro `test_size`). La primera vez no hay champion, así que el primer modelo entrenado pasa a producción.

Los archivos de los DAGs solo definen el flujo; la lógica está en `airflow/dags/utils/`, que no depende de Airflow. En los logs de las tareas aparece `No module named 'pendulum'`: es normal, Airflow revisa si el venv de ML tiene Airflow instalado y sigue igual.

### 5. API del modelo

La API (http://localhost:8800/docs) sirve la versión de `revenue_regressor` que tiene el alias `champion`:

| Endpoint | Qué hace |
|---|---|
| `POST /predict` | Recibe los datos de una película y devuelve la recaudación estimada en USD y la versión del modelo que la calculó. Devuelve 503 si todavía no hay champion. |
| `GET /health` | Estado del servicio (`ok` o `sin_modelo`) y versión cargada. |

No hace falta reiniciarla cuando el DAG promueve un modelo nuevo: cada 30 segundos (`MODEL_CHECK_SECONDS` en el `docker-compose.yaml`) revisa a qué versión apunta el alias y la carga si cambió. Si MLflow no responde, sigue usando el modelo que tiene cargado.

Ejemplo:

```bash
curl -X POST http://localhost:8800/predict -H "Content-Type: application/json" -d '{
  "budget": 25000000, "popularity": 16.3, "vote_count": 3427, "vote_average": 7.2,
  "runtime": 143, "release_year": 2006, "original_language": "en",
  "genres": ["Drama", "Crime"], "n_production_companies": 6
}'
```

### 6. Tests

```bash
uv run pytest
```

Prueban con datos sintéticos, sin levantar los servicios:

- `test_data.py`: limpieza, features y split del ETL.
- `test_model.py`: el pipeline del modelo.
- `test_api.py`: los endpoints, la recarga del champion, y que la API arme las features igual que el ETL.

### Apagar

```bash
docker compose --profile all down            # apaga; conserva datos
docker compose --profile all down --volumes  # apaga y BORRA bases de datos y buckets
```

---
