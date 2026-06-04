-- models/gold/fct_standings.sql
-- Este modelo toma los datos limpios de Silver y calcula
-- metricas de negocio utiles para el dashboard
-- Es la capa Gold de la arquitectura medallion

WITH silver_standings AS (
    -- ref() le dice a dbt que este modelo DEPENDE de stg_standings
    -- dbt nunca va a correr este modelo antes que stg_standings
    -- Eso es el DAG interno de dbt
    SELECT * FROM {{ ref('stg_standings') }}
),

enriched AS (
    SELECT
        -- Identificadores
        competition_code,
        season_id,
        position,
        team_id,
        team_name,
        team_short_name,
        extraction_date,

        -- Estadisticas base
        played_games,
        won,
        draw,
        lost,
        points,
        goals_for,
        goals_against,
        goal_difference,

        -- Metricas calculadas
        -- Porcentaje de victorias: de todos los partidos cuantos gane
        ROUND(SAFE_DIVIDE(won * 100.0, played_games), 1)
            AS win_percentage,

        -- Promedio de goles anotados por partido
        ROUND(SAFE_DIVIDE(goals_for * 1.0, played_games), 2)
            AS avg_goals_scored,

        -- Promedio de goles recibidos por partido
        ROUND(SAFE_DIVIDE(goals_against * 1.0, played_games), 2)
            AS avg_goals_conceded,

        -- Puntos por partido: indica rendimiento independiente
        -- de cuantos partidos lleva jugados
        ROUND(SAFE_DIVIDE(points * 1.0, played_games), 2)
            AS points_per_game,

        -- Clasificacion de forma: basado en puntos por partido
        CASE
            WHEN SAFE_DIVIDE(points * 1.0, played_games) >= 2.0
                THEN 'Excelente'
            WHEN SAFE_DIVIDE(points * 1.0, played_games) >= 1.5
                THEN 'Bueno'
            WHEN SAFE_DIVIDE(points * 1.0, played_games) >= 1.0
                THEN 'Regular'
            ELSE 'Malo'
        END AS team_form,

        -- Top 4: clasifica para Champions League
        CASE
            WHEN position <= 4 THEN TRUE
            ELSE FALSE
        END AS champions_league_spot,

        -- Zona de descenso: ultimos 3
        CASE
            WHEN position >= 18 THEN TRUE
            ELSE FALSE
        END AS relegation_zone,

        processed_at

    FROM silver_standings
)

SELECT * FROM enriched
ORDER BY competition_code, position
