from argparse import ArgumentParser
from datetime import datetime, date
from itertools import chain
import warnings
warnings.filterwarnings("ignore")


def get_datetime_info():
    """
    Return the current datetime and season year (i.e. for season 2025-2026, the year is 2025)
    """

    datetime_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    month_today = date.today().month
    year_today = date.today().year
    if month_today not in (9, 10, 11, 12):
        year_today -= 1
        
    return datetime_now, year_today


class SimpleLogger:
    
    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2

    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)

    def flush(self):
        pass