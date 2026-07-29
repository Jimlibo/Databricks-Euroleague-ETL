-- Create table if not exists (using LIMIT 0 to just establish schema)
CREATE TABLE IF NOT EXISTS workspace.euroleague.gold_player_stats AS
SELECT
  `player_id`,
  FIRST(`player` IGNORE NULLS) AS player,
  `season_code`,
  ROUND(AVG(`points`), 2) AS points_average,
  ROUND(AVG(CASE WHEN `two_points_attempted` > 0 THEN `two_points_made` * 1.0 / `two_points_attempted` ELSE NULL END), 2) AS two_points_percentage,
  ROUND(AVG(CASE WHEN `three_points_attempted` > 0 THEN `three_points_made` * 1.0 / `three_points_attempted` ELSE NULL END), 2) AS three_points_percentage,
  ROUND(AVG(CASE WHEN `free_throws_attempted` > 0 THEN `free_throws_made` * 1.0 / `free_throws_attempted` ELSE NULL END), 2) AS free_throw_percentage,
  ROUND(AVG(`offensive_rebounds`), 2) AS average_offensive_rebounds,
  ROUND(AVG(`defensive_rebounds`), 2) AS average_defensive_rebounds,
  ROUND(AVG(`offensive_rebounds` + `defensive_rebounds`), 2) AS average_total_rebounds,
  ROUND(AVG(`assists`), 2) AS average_assists,
  ROUND(AVG(`steals`), 2) AS average_steals
FROM
  `workspace`.`euroleague`.`silver_box_score`
WHERE
  `phase` = 'REGULARSEASON'
GROUP BY
  `player_id`, `season_code`
LIMIT 0;

-- Insert data (append mode)
WITH new_data AS (
  SELECT
    `player_id`,
    FIRST(`player` IGNORE NULLS) AS player,
    `season_code`,
    ROUND(AVG(`points`), 2) AS points_average,
    ROUND(AVG(CASE WHEN `two_points_attempted` > 0 THEN `two_points_made` * 1.0 / `two_points_attempted` ELSE NULL END), 2) AS two_points_percentage,
    ROUND(AVG(CASE WHEN `three_points_attempted` > 0 THEN `three_points_made` * 1.0 / `three_points_attempted` ELSE NULL END), 2) AS three_points_percentage,
    ROUND(AVG(CASE WHEN `free_throws_attempted` > 0 THEN `free_throws_made` * 1.0 / `free_throws_attempted` ELSE NULL END), 2) AS free_throw_percentage,
    ROUND(AVG(`offensive_rebounds`), 2) AS average_offensive_rebounds,
    ROUND(AVG(`defensive_rebounds`), 2) AS average_defensive_rebounds,
    ROUND(AVG(`offensive_rebounds` + `defensive_rebounds`), 2) AS average_total_rebounds,
    ROUND(AVG(`assists`), 2) AS average_assists,
    ROUND(AVG(`steals`), 2) AS average_steals
  FROM
    `workspace`.`euroleague`.`silver_box_score`
  WHERE
    `phase` = 'REGULARSEASON'
  GROUP BY
    `player_id`, `season_code`
)
MERGE INTO workspace.euroleague.gold_player_stats g
USING new_data n
ON g.`player_id` = n.`player_id` AND g.`season_code` = n.`season_code`
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;