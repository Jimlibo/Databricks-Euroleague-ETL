-- Create table if not exists (using LIMIT 0 to just establish schema)
CREATE TABLE IF NOT EXISTS workspace.euroleague.gold_last_game_stats AS
WITH last_game_stats AS (
  SELECT
    `player_id`,
    `player`,
    `points`,
    `total_rebounds`,
    `assists`,
    `steals`,
    `season_code`,
    `game_id`,
    `phase`,
    ROW_NUMBER() OVER (PARTITION BY `player_id` ORDER BY `game_id` DESC) AS rn
  FROM `workspace`.`euroleague`.`silver_box_score`
  WHERE `is_playing` = 1
),
last_game AS (
  SELECT *
  FROM last_game_stats
  WHERE rn = 1
),
player_averages AS (
  SELECT
    `player_id`,
    `season_code`,
    AVG(`points`) AS avg_points,
    AVG(`total_rebounds`) AS avg_total_rebounds,
    AVG(`assists`) AS avg_assists,
    AVG(`steals`) AS avg_steals
  FROM `workspace`.`euroleague`.`silver_box_score`
  WHERE `is_playing` = 1 AND `phase` = 'REGULARSEASON'
  GROUP BY `player_id`, `season_code`
)
SELECT
  lg.`player_id`,
  lg.`player`,
  lg.`season_code`,
  lg.`game_id` AS last_game_id,
  lg.`phase`,
  lg.`points` AS last_total_points,
  lg.`total_rebounds` AS last_total_rebounds,
  lg.`assists` AS last_total_assists,
  lg.`steals` AS last_total_steals,
  ROUND(pa.`avg_points`, 2) AS avg_total_points,
  ROUND(pa.`avg_total_rebounds`, 2) AS avg_total_rebounds,
  ROUND(pa.`avg_assists`, 2) AS avg_total_assists,
  ROUND(pa.`avg_steals`, 2) AS avg_total_steals,
  ROUND(lg.`points` - pa.`avg_points`, 2) AS diff_points,
  ROUND(lg.`total_rebounds` - pa.`avg_total_rebounds`, 2) AS diff_total_rebounds,
  ROUND(lg.`assists` - pa.`avg_assists`, 2) AS diff_total_assists,
  ROUND(lg.`steals` - pa.`avg_steals`, 2) AS diff_total_steals,
  ROUND(
    try_divide(
      (lg.`points` * pa.`avg_points` + lg.`total_rebounds` * pa.`avg_total_rebounds` + lg.`assists` * pa.`avg_assists` + lg.`steals` * pa.`avg_steals`),
      (
        SQRT(POWER(lg.`points`, 2) + POWER(lg.`total_rebounds`, 2) + POWER(lg.`assists`, 2) + POWER(lg.`steals`, 2)) *
        SQRT(POWER(pa.`avg_points`, 2) + POWER(pa.`avg_total_rebounds`, 2) + POWER(pa.`avg_assists`, 2) + POWER(pa.`avg_steals`, 2))
      )
    ), 4
  ) AS season_avg_similarity
FROM last_game lg
JOIN player_averages pa ON lg.`player_id` = pa.`player_id` AND lg.`season_code` = pa.`season_code`
LIMIT 0;

-- Insert data (upsert by player_id)
WITH last_game_stats AS (
  SELECT
    `player_id`,
    `player`,
    `points`,
    `total_rebounds`,
    `assists`,
    `steals`,
    `season_code`,
    `game_id`,
    `phase`,
    ROW_NUMBER() OVER (PARTITION BY `player_id` ORDER BY `game_id` DESC) AS rn
  FROM `workspace`.`euroleague`.`silver_box_score`
  WHERE `is_playing` = 1
),
last_game AS (
  SELECT *
  FROM last_game_stats
  WHERE rn = 1
),
player_averages AS (
  SELECT
    `player_id`,
    `season_code`,
    AVG(`points`) AS avg_points,
    AVG(`total_rebounds`) AS avg_total_rebounds,
    AVG(`assists`) AS avg_assists,
    AVG(`steals`) AS avg_steals
  FROM `workspace`.`euroleague`.`silver_box_score`
  WHERE `is_playing` = 1 AND `phase` = 'REGULARSEASON'
  GROUP BY `player_id`, `season_code`
),
new_data AS (
  SELECT
    lg.`player_id`,
    lg.`player`,
    lg.`season_code`,
    lg.`game_id` AS last_game_id,
    lg.`phase`,
    lg.`points` AS last_total_points,
    lg.`total_rebounds` AS last_total_rebounds,
    lg.`assists` AS last_total_assists,
    lg.`steals` AS last_total_steals,
    ROUND(pa.`avg_points`, 2) AS avg_total_points,
    ROUND(pa.`avg_total_rebounds`, 2) AS avg_total_rebounds,
    ROUND(pa.`avg_assists`, 2) AS avg_total_assists,
    ROUND(pa.`avg_steals`, 2) AS avg_total_steals,
    ROUND(lg.`points` - pa.`avg_points`, 2) AS diff_points,
    ROUND(lg.`total_rebounds` - pa.`avg_total_rebounds`, 2) AS diff_total_rebounds,
    ROUND(lg.`assists` - pa.`avg_assists`, 2) AS diff_total_assists,
    ROUND(lg.`steals` - pa.`avg_steals`, 2) AS diff_total_steals,
    ROUND(
    try_divide(
      (lg.`points` * pa.`avg_points` + lg.`total_rebounds` * pa.`avg_total_rebounds` + lg.`assists` * pa.`avg_assists` + lg.`steals` * pa.`avg_steals`),
      (
        SQRT(POWER(lg.`points`, 2) + POWER(lg.`total_rebounds`, 2) + POWER(lg.`assists`, 2) + POWER(lg.`steals`, 2)) *
        SQRT(POWER(pa.`avg_points`, 2) + POWER(pa.`avg_total_rebounds`, 2) + POWER(pa.`avg_assists`, 2) + POWER(pa.`avg_steals`, 2))
      )
    ), 4
  ) AS season_avg_similarity
  FROM last_game lg
  JOIN player_averages pa ON lg.`player_id` = pa.`player_id` AND lg.`season_code` = pa.`season_code`
)
MERGE INTO workspace.euroleague.gold_last_game_stats AS g
USING new_data AS n
ON g.`player_id` = n.`player_id` AND g.`season_code` = n.`season_code`
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;