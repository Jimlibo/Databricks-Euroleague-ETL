import sys 
import os
from scrapper import EuroleagueScrapper
from config import DELTA_SILVER_PATH
from ingestion_utils import get_datetime_info, SimpleLogger
from spark_utils import init_euroleague_spark_session, write_delta
import warnings
warnings.filterwarnings("ignore")


def ingest_data(game_code_start: int = 1):
    """
    Load game data for euroleague games, starting from game with code `game_code_start`
    and ending with the last game of the current season.
    """
    # initialize spark session
    spark = init_euroleague_spark_session()

    # get datetime info
    datetime_now, year_today = get_datetime_info()

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
    box_score_spark = spark.createDataFrame(tables["box_score"])
    header_spark = spark.createDataFrame(tables["header"])
    comparison_spark = spark.createDataFrame(tables["comparison"])

    # store spark dfs to delta lake tables
    write_delta(box_score_spark, f"{DELTA_SILVER_PATH}_box_score", "game_player_id")
    write_delta(header_spark, f"{DELTA_SILVER_PATH}_header", "game_id")
    write_delta(comparison_spark, f"{DELTA_SILVER_PATH}_comparison", "game_id")


if __name__ == '__main__':
    ingest_data()