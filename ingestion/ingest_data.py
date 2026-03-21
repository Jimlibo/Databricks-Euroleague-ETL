import sys 
import os
from ingestion_utils import get_datetime_info, SimpleLogger
from scrapper import EuroleagueScrapper
import warnings
warnings.filterwarnings("ignore")


def ingest_data(game_code_start: int = 1):
    """
    Load game data for euroleague games, starting from game with code `game_code_start`
    and ending with the last game of the current season.
    """
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
        failed_extractions_limit=500, 
        game_code_start=game_code_start, 
        datetime_now=datetime_now,
    )
    euroleague_scrapper.implement_scrapping_process()


if __name__ == '__main__':
    ingest_data()