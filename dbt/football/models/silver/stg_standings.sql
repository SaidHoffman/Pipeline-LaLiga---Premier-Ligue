WITH raw_standings AS (
    SELECT *
    FROM {{ source('football_raw', 'standings') }}
),

cleaned AS (
    SELECT
        competition_code,
        season_id,
        CAST(position AS INT64)          AS position,
        team_id,
        team_name,
        team_short_name,
        CAST(played_games AS INT64)      AS played_games,
        CAST(won AS INT64)               AS won,
        CAST(draw AS INT64)              AS draw,
        CAST(lost AS INT64)              AS lost,
        CAST(points AS INT64)            AS points,
        CAST(goals_for AS INT64)         AS goals_for,
        CAST(goals_against AS INT64)     AS goals_against,
        CAST(goal_difference AS INT64)   AS goal_difference,
        extraction_date,
        CURRENT_TIMESTAMP()              AS processed_at
    FROM raw_standings
    WHERE team_id IS NOT NULL
        AND team_name IS NOT NULL
)

SELECT * FROM cleaned
