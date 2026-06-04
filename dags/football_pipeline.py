from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'said',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

def extraer_premier_league(**context):
    """
    Extrae datos de la Premier League y los guarda en GCS.
    context['ds'] es la fecha de ejecucion del DAG en formato YYYY-MM-DD.
    Airflow lo pasa automaticamente.
    """
    from data.extract_football import extraer_y_guardar
    fecha = context['ds']
    print(f"Extrayendo datos para fecha: {fecha}")
    resultado = extraer_y_guardar("PL", fecha)
    return resultado

def extraer_la_liga(**context):
    """
    Extrae datos de La Liga.
    """
    from data.extract_football import extraer_y_guardar
    fecha = context['ds']
    print(f"Extrayendo La Liga para fecha: {fecha}")
    resultado = extraer_y_guardar("PD", fecha)
    return resultado

def validar_datos(**context):
    """
    Verifica que los archivos llegaron a GCS correctamente.
    Si no llegaron, falla con mensaje claro antes de continuar.
    Esto es el fail fast que vimos en teoria.
    """
    from google.cloud import storage
    import os

    fecha = context['ds']
    bucket_name = os.getenv("GCS_BUCKET", "football-pipeline-raw")
    client = storage.Client(project=os.getenv("GCP_PROJECT_ID"))
    bucket = client.bucket(bucket_name)

    archivos_requeridos = [
        f"bronze/PL/{fecha}/standings.json",
        f"bronze/PL/{fecha}/matches.json",
        f"bronze/PD/{fecha}/standings.json",
        f"bronze/PD/{fecha}/matches.json",
    ]

    archivos_faltantes = []
    for archivo in archivos_requeridos:
        blob = bucket.blob(archivo)
        if not blob.exists():
            archivos_faltantes.append(archivo)

    if archivos_faltantes:
        raise Exception(
            f"Validacion fallida. Archivos faltantes en GCS:\n"
            + "\n".join(archivos_faltantes)
        )

    print(f"Validacion OK: todos los archivos presentes para {fecha}")

with DAG(
    dag_id='football_pipeline',
    default_args=default_args,
    description='Pipeline de datos de futbol: PL y La Liga',
    schedule_interval='0 7 * * *',  # Todos los dias a las 7am
    start_date=datetime(2026, 6, 1),
    catchup=False,  # No ejecutar dias pasados
    tags=['football', 'data-engineering'],
) as dag:

    extraer_pl = PythonOperator(
        task_id='extraer_premier_league',
        python_callable=extraer_premier_league,
    )

    extraer_pd = PythonOperator(
        task_id='extraer_la_liga',
        python_callable=extraer_la_liga,
    )

    validar = PythonOperator(
        task_id='validar_datos_gcs',
        python_callable=validar_datos,
    )

    # Premier League y La Liga se extraen en PARALELO
    # Luego validamos que ambas llegaron bien
    [extraer_pl, extraer_pd] >> validar
