import requests
import json
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FOOTBALL_API_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BASE_URL = "http://api.football-data.org/v4"

HEADERS = {"X-Auth-Token": API_KEY}

def get_standings(competition_code: str) -> dict:
    """
    Obtiene la tabla de posiciones de una liga.
    competition_code: PL=Premier League, PD=La Liga, CL=Champions League
    """
    url = f"{BASE_URL}/competitions/{competition_code}/standings"
    response = requests.get(url, headers=HEADERS)
    
    # Si la API nos limita, lanzamos error claro
    if response.status_code == 429:
        raise Exception("Rate limit alcanzado. Espera un minuto.")
    
    if response.status_code != 200:
        raise Exception(f"Error API: {response.status_code} - {response.text}")
    
    return response.json()

def get_matches(competition_code: str, matchday: int = None) -> dict:
    """
    Obtiene los partidos de una liga.
    """
    url = f"{BASE_URL}/competitions/{competition_code}/matches"
    if matchday:
        url += f"?matchday={matchday}"
    
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code == 429:
        raise Exception("Rate limit alcanzado. Espera un minuto.")
    
    if response.status_code != 200:
        raise Exception(f"Error API: {response.status_code} - {response.text}")
    
    return response.json()

def save_to_gcs(data: dict, blob_name: str) -> str:
    """
    Guarda datos JSON en GCS (nuestra capa Bronze).
    Retorna la ruta completa del archivo guardado.
    """
    client = storage.Client(project=os.getenv("GCP_PROJECT_ID"))
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    
    # Guardamos como JSON con formato legible
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
    
    ruta = f"gs://{GCS_BUCKET}/{blob_name}"
    print(f"Guardado en: {ruta}")
    return ruta

def extraer_y_guardar(competition_code: str, fecha: str = None) -> dict:
    """
    Funcion principal: extrae datos de la API y los guarda en GCS.
    Esta es la funcion que va a llamar Airflow.
    fecha: formato YYYY-MM-DD, por defecto hoy
    """
    if not fecha:
        fecha = datetime.now().strftime("%Y-%m-%d")
    
    resultados = {}
    
    # Extraer standings
    print(f"Extrayendo standings de {competition_code}...")
    standings = get_standings(competition_code)
    
    # Guardar en Bronze con ruta organizada por fecha
    # Esto es arquitectura medallion: bronze/competicion/fecha/archivo.json
    blob_standings = f"bronze/{competition_code}/{fecha}/standings.json"
    resultados["standings"] = save_to_gcs(standings, blob_standings)
    
    # Extraer partidos
    print(f"Extrayendo partidos de {competition_code}...")
    matches = get_matches(competition_code)
    
    blob_matches = f"bronze/{competition_code}/{fecha}/matches.json"
    resultados["matches"] = save_to_gcs(matches, blob_matches)
    
    print(f"Extraccion completada: {resultados}")
    return resultados

if __name__ == "__main__":
    # Para probar manualmente
    extraer_y_guardar("PL")  # Premier League
