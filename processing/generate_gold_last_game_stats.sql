-- Create table if not exists (using LIMIT 0 to just establish schema)
CREATE TABLE IF NOT EXISTS workspace.euroleague.gold_last_game_stats AS
WITH player_windowed AS (
  SELECT
    `player_id`,
    `player`,
    `points`,
    `total_rebounds`,
    `assists`,
    `steals`,
    `season_code`,
    `round`,
    `game_id`,
    `phase`,
    ROW_NUMBER() OVER (PARTITION BY `player_id` ORDER BY `game_id` DESC) AS rn,   -- column for finding last game for each player
    AVG(`points`)        OVER (PARTITION BY `player_id`) AS avg_points,
    AVG(`total_rebounds`) OVER (PARTITION BY `player_id`) AS avg_total_rebounds,
    AVG(`assists`)       OVER (PARTITION BY `player_id`) AS avg_assists,
    AVG(`steals`)        OVER (PARTITION BY `player_id`) AS avg_steals
  FROM `workspace`.`euroleague`.`silver_box_score`
  WHERE `is_playing` = 1    -- consider only games where player was playing
),
last_game AS (
  SELECT *
  FROM player_windowed
  WHERE rn = 1
)
SELECT
  `player_id`,
  `player`,
  `season_code`,
  `game_id` AS last_game_id,
  `phase`,
  `points`        AS last_total_points,
  `total_rebounds` AS last_total_rebounds,
  `assists`       AS last_total_assists,
  `steals`        AS last_total_steals,
  ROUND(`avg_points`, 2)        AS avg_total_points,
  ROUND(`avg_total_rebounds`, 2) AS avg_total_rebounds,
  ROUND(`avg_assists`, 2)       AS avg_total_assists,
  ROUND(`avg_steals`, 2)        AS avg_total_steals,
  ROUND(`points` - `avg_points`, 2)             AS diff_points,
  ROUND(`total_rebounds` - `avg_total_rebounds`, 2) AS diff_total_rebounds,
  ROUND(`assists` - `avg_assists`, 2)           AS diff_total_assists,
  ROUND(`steals` - `avg_steals`, 2)             AS diff_total_steals
FROM last_game
LIMIT 0;

-- Insert data (upsert by player_id)
WITH player_windowed AS (
  SELECT
    `player_id`,
    `player`,
    `points`,
    `total_rebounds`,
    `assists`,
    `steals`,
    `season_code`,
    `round`,
    `game_id`,
    `phase`,
    ROW_NUMBER() OVER (PARTITION BY `player_id` ORDER BY `game_id` DESC) AS rn,
    AVG(`points`)        OVER (PARTITION BY `player_id`) AS avg_points,
    AVG(`total_rebounds`) OVER (PARTITION BY `player_id`) AS avg_total_rebounds,
    AVG(`assists`)       OVER (PARTITION BY `player_id`) AS avg_assists,
    AVG(`steals`)        OVER (PARTITION BY `player_id`) AS avg_steals
  FROM `workspace`.`euroleague`.`silver_box_score`
  WHERE `is_playing` = 1
),
last_game AS (
  SELECT *
  FROM player_windowed
  WHERE rn = 1
),
new_data AS (
  SELECT
    `player_id`,
    `player`,
    `season_code`,
    `game_id` AS last_game_id,
    `phase`,
    `points` AS last_total_points,
    `total_rebounds` AS last_total_rebounds,
    `assists` AS last_total_assists,
    `steals` AS last_total_steals,
    ROUND(`avg_points`, 2) AS avg_total_points,
    ROUND(`avg_total_rebounds`, 2) AS avg_total_rebounds,
    ROUND(`avg_assists`, 2) AS avg_total_assists,
    ROUND(`avg_steals`, 2) AS avg_total_steals,
    ROUND(`points` - `avg_points`, 2) AS diff_points,
    ROUND(`total_rebounds` - `avg_total_rebounds`, 2) AS diff_total_rebounds,
    ROUND(`assists` - `avg_assists`, 2) AS diff_total_assists,
    ROUND(`steals` - `avg_steals`, 2) AS diff_total_steals
  FROM last_game
)
MERGE INTO workspace.euroleague.gold_last_game_stats AS g
USING new_data AS n
ON g.`player_id` = n.`player_id`
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;