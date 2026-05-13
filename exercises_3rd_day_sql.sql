--  sql sum
SELECT SUM(v) AS sum
FROM elements;

-- sql events delta
WITH ranked AS (
    SELECT
        event_type,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY event_type
            ORDER BY time DESC
        ) AS rn
    FROM events
),
top_two AS (
    SELECT
        event_type,
        MAX(CASE WHEN rn = 1 THEN value END) AS latest_value,
        MAX(CASE WHEN rn = 2 THEN value END) AS second_latest_value
    FROM ranked
    WHERE rn <= 2
    GROUP BY event_type
)
SELECT
    event_type,
    (latest_value - second_latest_value) AS value
FROM top_two
WHERE second_latest_value IS NOT NULL
ORDER BY event_type;

-- sql world cup
SELECT
    t.team_id AS team_id,
    t.team_name AS team_name,
    COALESCE(SUM(m.points), 0) AS num_points
FROM teams t
LEFT JOIN (
    SELECT
        host_team AS team_id,
        CASE
            WHEN host_goals > guest_goals THEN 3
            WHEN host_goals = guest_goals THEN 1
            ELSE 0
        END AS points
    FROM matches

    UNION ALL

    SELECT
        guest_team AS team_id,
        CASE
            WHEN guest_goals > host_goals THEN 3
            WHEN guest_goals = host_goals THEN 1
            ELSE 0
        END AS points
    FROM matches
) m
ON t.team_id = m.team_id
GROUP BY t.team_id, t.team_name
ORDER BY num_points DESC, t.team_id ASC;