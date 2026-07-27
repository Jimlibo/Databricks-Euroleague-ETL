import sys 
import os
from scrapper import EuroleagueScrapper
from config import DELTA_SILVER_PATH
from ingestion_utils import get_datetime_info, SimpleLogger
from spark_utils import init_euroleague_spark_session, write_delta
import warnings
warnings.filterwarnings("ignore")


def ingest_data(game_code_start: int | None = None):
    """
    Load game data for euroleague games, starting from game with code `game_code_start`
    and ending with the last game of the current season. If `game_code_start` is not provided,
    the function will start from the last game that was ingested.
    """
    # initialize spark session
    spark = init_euroleague_spark_session()

    # get datetime info
    datetime_now, year_today = get_datetime_info()

    # if game_code_start was not provided, set it to the last game that was ingested
    if game_code_start is None:
        # initialize game_code_start with default value
        game_code_start = 1
        # find the last game that was ingested
        table_name = "workspace.euroleague.silver_header"
        if spark.catalog.tableExists(table_name):
            last_game_id = (
                spark.read.table(table_name)
                .filter(f"season_code = 'E{year_today}'")
                .orderBy("game_id", ascending=False)
                .select("game_id")
                .first()
            )

            if last_game_id is not None:
                try:
                    game_code_start = int(last_game_id.game_id.split("_")[1]) + 1
                    print(
                        f"Found last ingested game_id: {last_game_id.game_id} -- starting from game_id: {game_code_start}"
                    )
                except ValueError:
                    game_code_start = 1

    # create a simple logfile
    if not os.path.exists("simple_logs"):
        os.makedirs("simple_logs")
    sys.stdout = SimpleLogger(open(f"simple_logs/logfile_{datetime_now}", "w"), sys.stdout)

    # initialize EuroleagueScrapper and implement scrapping process for euroleague data
    euroleague_scrapper = EuroleagueScrapper(
        competition="euroleague", 
        season_codes=[f"E{year_today}"], 
        euroleague_apis=["Header", "Boxscore", "Comparison"], 
        failed_extractions_limit=50, 
        game_code_start=game_code_start, 
        datetime_now=datetime_now,
    )
    euroleague_scrapper.implement_scrapping_process()

    # extract tables and convert them to spark dataframes
    tables = euroleague_scrapper.build_final_tables()
    box_score_spark = spark.createDataFrame(tables["box_score"]) if tables["box_score"] is not None else None
    header_spark = spark.createDataFrame(tables["header"]) if tables["header"] is not None else None
    comparison_spark = spark.createDataFrame(tables["comparison"]) if tables["comparison"] is not None else None

    # store spark dfs to delta lake tables
    write_delta(box_score_spark, f"{DELTA_SILVER_PATH}_box_score")
    write_delta(header_spark, f"{DELTA_SILVER_PATH}_header")
    write_delta(comparison_spark, f"{DELTA_SILVER_PATH}_comparison")


if __name__ == '__main__':
    ingest_data()