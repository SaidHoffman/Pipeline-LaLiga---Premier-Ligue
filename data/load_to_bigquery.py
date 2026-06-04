import json
import os
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "football_raw")

def parse_standings(data: dict, competition_code: str, fecha: str) -> list:
    """
    Transforma el JSON crudo de standings en filas planas para BigQuery.
    El JSON tiene estructura anidada, necesitamos aplanarlo.
    """
    rows = []
    
    season_id = data.get("season", {}).get("id")
    
    # La API devuelve standings como lista de tipos (TOTAL, HOME, AWAY)
    # Solo nos interesa TOTAL
    for standing in data.get("standings", []):
        if standing.get("type") != "TOTAL":
            continue
            
        for team_entry in standing.get("table", []):
            team = team_entry.get("team", {})
            
            row = {
                "competition_code": competition_code,
                "season_id": str(season_id),
                "position": team_entry.get("position"),
                "team_id": str(team.get("id")),
                "team_name": team.get("name"),
                "team_short_name": team.get("shortName"),
                "played_games": team_entry.get("playedGames"),
                "won": team_entry.get("won"),
                "draw": team_entry.get("draw"),
                "lost": team_entry.get("lost"),
                "points": team_entry.get("points"),
                "goals_for": team_entry.get("goalsFor"),
                "goals_against": team_entry.get("goalsAgainst"),
                "goal_difference": team_entry.get("goalDifference"),
                "extraction_date": fecha,
            }
            rows.append(row)
    
    return rows

def load_standings_to_bq(competition_code: str, fecha: str) -> int:
    """
    Lee standings de GCS y los carga a BigQuery.
    Retorna el numero de filas cargadas.
    """
    # Leer JSON de GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob_name = f"bronze/{competition_code}/{fecha}/standings.json"
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        raise FileNotFoundError(f"No existe: gs://{GCS_BUCKET}/{blob_name}")
    
    data = json.loads(blob.download_as_string())
    
    # Parsear y aplanar el JSON
    rows = parse_standings(data, competition_code, fecha)
    
    if not rows:
        raise ValueError(f"No se encontraron standings en {blob_name}")
    
    # Cargar a BigQuery
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET_RAW}.standings"
    
    # UPSERT: borramos la particion del dia antes de insertar
    # Esto garantiza idempotencia: si corremos dos veces, no duplicamos
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE competition_code = '{competition_code}'
        AND extraction_date = '{fecha}'
    """
    bq_client.query(delete_query).result()
    
    # Insertar filas nuevas
    errors = bq_client.insert_rows_json(table_id, rows)
    
    if errors:
        raise Exception(f"Errores cargando a BigQuery: {errors}")
    
    print(f"Cargadas {len(rows)} filas de {competition_code} para {fecha}")
    return len(rows)

def cargar_todas_las_ligas(fecha: str = None) -> dict:
    """
    Funcion principal que carga todas las ligas.
    Esta es la que va a llamar Airflow.
    """
    from datetime import datetime
    if not fecha:
        fecha = datetime.now().strftime("%Y-%m-%d")
    
    resultados = {}
    for competition in ["PL", "PD"]:
        try:
            filas = load_standings_to_bq(competition, fecha)
            resultados[competition] = {"status": "ok", "filas": filas}
        except Exception as e:
            resultados[competition] = {"status": "error", "error": str(e)}
            raise
    
    return resultados

if __name__ == "__main__":
    from datetime import datetime
    fecha = datetime.now().strftime("%Y-%m-%d")
    cargar_todas_las_ligas(fecha)
