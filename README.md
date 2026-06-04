# Football Data Pipeline 

Pipeline de datos end-to-end que extrae estadísticas de fútbol en tiempo real,
las procesa con arquitectura medallion, y las visualiza en un dashboard interactivo.

## Arquitectura

API football-data.org
↓
Google Cloud Storage (Bronze - datos crudos)
↓
BigQuery Raw (carga con idempotencia)
↓
dbt Silver (limpieza y tipado)
↓
dbt Gold (métricas de negocio)
↓
Looker Studio Dashboard

## Stack tecnológico

- **Orquestación:** Apache Airflow 2.8 en Docker
- **Data Lake:** Google Cloud Storage (arquitectura medallion)
- **Warehouse:** BigQuery
- **Transformaciones:** dbt con tests de calidad de datos
- **Dashboard:** Looker Studio
- **Infraestructura:** Docker Compose, GCP

## Decisiones de arquitectura

**¿Por qué arquitectura medallion?**
Separar Bronze/Silver/Gold permite reprocesar datos históricos en cualquier
momento sin perder el dato original. Si hay un bug en la transformación,
el dato crudo en Bronze siempre está disponible para backfill.

**¿Por qué idempotencia en la carga?**
El script de carga ejecuta DELETE + INSERT por fecha y competición antes
de insertar. Si el pipeline falla y reintenta, el resultado es idéntico.
Sin duplicados, sin datos corruptos.

**¿Por qué dbt para transformaciones?**
dbt permite versionar las transformaciones SQL, documentarlas, y añadir
tests de calidad de datos que corren automáticamente. Si un test falla,
el pipeline se detiene antes de que datos incorrectos lleguen al dashboard.

**¿Por qué tareas en paralelo en Airflow?**
La extracción de Premier League y La Liga son independientes entre sí.
Correrlas en paralelo reduce el tiempo total del pipeline a la mitad.

## Data Quality Tests

7 tests automáticos con dbt:
- `team_id`: unique + not_null
- `competition_code`: not_null + accepted_values
- `points`: not_null
- `played_games`: not_null
- `position`: not_null

## Cómo ejecutar

### Requisitos
- Docker y Docker Compose
- Python 3.12+
- Cuenta de GCP con BigQuery y Cloud Storage habilitados

### Setup

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/football-pipeline
cd football-pipeline

# Crear entorno virtual
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# Autenticar GCP
gcloud auth application-default login --no-launch-browser

# Levantar Airflow
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler -d
```

### Ejecutar pipeline manualmente

Abrir http://localhost:8080, activar el DAG `football_pipeline`
y ejecutar con el botón Trigger DAG.

### Ejecutar transformaciones dbt

```bash
cd dbt/football
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Estructura del proyecto
football-pipeline/
├── dags/
│   └── football_pipeline.py    # DAG de Airflow
├── data/
│   ├── extract_football.py     # Extracción de API → GCS
│   └── load_to_bigquery.py     # Carga GCS → BigQuery
├── dbt/football/
│   └── models/
│       ├── silver/             # Limpieza de datos
│       └── gold/               # Métricas de negocio
└── docker-compose.yml


## Ligas disponibles

- Premier League (PL)
- La Liga (PD)

