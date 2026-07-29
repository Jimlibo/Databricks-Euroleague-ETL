import requests
import json
import os
import pandas as pd
from time import time
from pyspark.sql import Row, SparkSession
from config import RAW_PATH, DELTA_BRONZE_PATH
from spark_utils import write_delta
import warnings
warnings.filterwarnings("ignore")


class EuroleagueScrapper:
    def __init__(self, competition, season_codes, euroleague_apis, failed_extractions_limit, game_code_start, datetime_now):
        # initialize api call parameters
        self.competition = competition
        self.season_codes = season_codes
        self.euroleague_apis = euroleague_apis
        self.failed_extractions_limit = failed_extractions_limit
        self.game_code_start = game_code_start
        self.datetime_now = datetime_now

        # initialize lists for api results
        self.players_df_list = []
        self.games_df_list = []
        self.comparison_df_list = []

        # initialize spark session
        self.spark = SparkSession.getActiveSession()
        
    def implement_scrapping_process(self):
        '''
        This is the main instance method of the class. 
        It makes use of the remaining instance methods and provides a step by step implementation of the process.
        '''  
        # create directory for raw data storage
        os.makedirs(RAW_PATH, exist_ok=True)

        # implement process per season 
        for index, sc in enumerate(self.season_codes):
            # initialize season df rows
            season_api_rows = {api: [] for api in self.euroleague_apis}
            # initialize basic variable of process
            process_start_time, season_end, meta_data_dict, gc = self.initialize_process(index, sc)

            while not season_end:

                # update the game_code counter
                gc += 1 

                # scrap APIs, load data and update info
                meta_data_dict, api_rows = self.request_extract_load_update(gc, sc, process_start_time, meta_data_dict)

                # update season df rows
                for api in self.euroleague_apis:
                    season_api_rows[api].extend(api_rows.get(api, []))

                # save meta_data
                with open(fr"{RAW_PATH}{self.competition}_json/{sc}/{sc}_meta_data_{self.datetime_now}.json", "w") as output_file:
                    json.dump(meta_data_dict, output_file)

                # update flag --- the 1st condition is obvious --- the 2nd condition reflects a possible problem with the URLs or a premature end of the season (e.g. covid season E2019)
                if (meta_data_dict[sc]["number_of_FinalFour_games"] == 4) or (meta_data_dict[sc]["number_of_failed_extractions"] > self.failed_extractions_limit):
                    season_end = True

            # store season data to df
            for api in self.euroleague_apis:
                if len(season_api_rows[api]) > 0:
                    df = self.spark.createDataFrame(season_api_rows[api])
                    write_delta(df, f"{DELTA_BRONZE_PATH}_{api.lower()}", mode="overwrite")
                else:
                    print(f"Empty {api} data for {sc} season")


    def initialize_process(self, index, sc):
        
        # initialize timer and flag 
        process_start_time = time()
        season_end = False

        # only the first season_code can start with game_code > 1 
        if index > 0:
            self.game_code_start = 1

        # initialize the meta_data dictionary
        meta_data_dict = {sc: {"competition": self.competition,
                               "total_number_of_games": 0,
                               "number_of_RegularSeason_games": 0,
                               "number_of_Top32_games": 0,
                               "number_of_Top16_games": 0,
                               "number_of_Playoff_games": 0,
                               "number_of_FinalFour_games": 0,
                               "number_of_QuarterFinals_games": 0,
                               "number_of_SemiFinals_games": 0,
                               "number_of_Finals_games": 0,
                               "number_of_failed_extractions": 0,
                               "limit_of_failed_extractions": self.failed_extractions_limit,
                               "game_code_counter": None,
                               "game_code_start": self.game_code_start,
                               "unknown_Phase": [],
                               "time_counter": None}}

        # create directories
        os.makedirs(fr"{RAW_PATH}{self.competition}_json/{sc}/success", exist_ok=True)
        os.makedirs(fr"{RAW_PATH}{self.competition}_json/{sc}/failure", exist_ok=True)

        # initialize the game_code counter
        gc = self.game_code_start - 1
        
        return process_start_time, season_end, meta_data_dict, gc

    def request_extract_load_update(self, gc, sc, process_start_time, meta_data_dict):
        # initialize the api_rows dictionary
        api_rows = {api: [] for api in self.euroleague_apis}   
        # fetch results from each api     
        for api in self.euroleague_apis:
            # HTTP request
            url = f"https://live.euroleague.net/api/{api}?gamecode={str(gc)}&seasoncode={sc}"
            response = requests.get(url)
            response_status = response.status_code

            try:
                # extract data
                response_dict = response.json()
                # store raw api data to df row
                row = Row(
                    season_code=sc,
                    game_code=gc,
                    api=api,
                    ingestion_time=str(self.datetime_now),
                    raw_json=json.dumps(response_dict)
                )
                api_rows[api].append(row)
                # df = self.spark.createDataFrame([row])
                # write_delta(df, f"{DELTA_BRONZE_PATH}_{api.lower()}")

                # based on which api was used, handle the response accordingly
                if api == "Header":
                    Phase, Round, Date, meta_data_dict = self.get_info_from_header_api(
                        response_dict, meta_data_dict, url, gc, sc, process_start_time
                    )
                    self.process_header(response_dict, sc, gc, Phase, Round)
                elif api == "Boxscore":
                    self.process_boxscore(response_dict, sc, gc, Phase, Round, Date)
                elif api == "Comparison":
                    self.process_comparison(response_dict, sc, gc, Phase, Round)

            except Exception as e:
                print(e, "--- URL:", url)
                # udpate the failure counter of the meta_data dictionary
                meta_data_dict[sc]["number_of_failed_extractions"] += 1
                # update the failure file by saving the failed URL and the status_code of the responce
                with open(fr"{RAW_PATH}{self.competition}_json/{sc}/failure/{sc}_failed_extractions_{self.datetime_now}.txt", 'a') as failure_file:
                    failure_file.write(f"failed_url: {url}  ---  status_code: {str(response_status)}\n")
                    failure_file.close()
                    
            # update the time and game_code counters of the meta_data dictionary
            meta_data_dict[sc]["time_counter"] = f"{round((time() - process_start_time) / 60, 1)} minutes"
            meta_data_dict[sc]["game_code_counter"] = gc
                    
        return meta_data_dict,  api_rows
    
    def get_info_from_header_api(self, response_dict, meta_data_dict, url, gc, sc, process_start_time):
    
        # get useful info from Header API
        Round = "{:02d}".format(int(response_dict["Round"]))
        Date_split = response_dict["Date"].split("/")
        Date = "".join([Date_split[2], Date_split[1], Date_split[0]])
        Phase_split = response_dict["Phase"].lower().split(" ")
        if len(Phase_split) > 1:
            Phase = "".join([Phase_split[0].capitalize(), Phase_split[1].capitalize()])
        else:
            Phase = Phase_split[0].capitalize()

        # update the game counters of the meta_data dictionary    
        meta_data_dict[sc]["total_number_of_games"] += 1
        if Phase.startswith("Reg"):
            meta_data_dict[sc]["number_of_RegularSeason_games"] += 1
        elif Phase.startswith("Last32"):
            meta_data_dict[sc]["number_of_Top32_games"] += 1
        elif Phase.startswith("Top") or Phase.startswith("Last16") or Phase.startswith("Eight"):
            meta_data_dict[sc]["number_of_Top16_games"] += 1
        elif Phase.startswith("Play"):
            meta_data_dict[sc]["number_of_Playoff_games"] += 1
        elif Phase.startswith("FinalFour"):
            meta_data_dict[sc]["number_of_FinalFour_games"] += 1
        elif Phase.startswith("Quarter"):
            meta_data_dict[sc]["number_of_QuarterFinals_games"] += 1
        elif Phase.startswith("Semi"):
            meta_data_dict[sc]["number_of_SemiFinals_games"] += 1
        elif Phase.startswith("Finals"):
            meta_data_dict[sc]["number_of_Finals_games"] += 1
        else:
            meta_data_dict[sc]["unknown_Phase"].append([Phase, url])

        # print a message for monitoring purposes
        if gc == 1:
            print("")
        print(f"SeasonCode: {sc}  ---  Phase: {Phase}  ---  Round: {Round}  ---  "
              f"GameCode:", "{:03d}".format(gc), " ---  TimeCounter:", round((time() - process_start_time) / 60, 1), "min")
            
        return Phase, Round, Date, meta_data_dict
    
    def process_header(self, data, season, gamecode, phase, round_):
        # create unique game_id
        game_id = f"{season}_{str(gamecode).zfill(3)}"

        # Teams
        team_a = data.get("TeamA")
        team_b = data.get("TeamB")
        team_id_a = data.get("CodeTeamA")
        team_id_b = data.get("CodeTeamB")
        game = f"{team_id_a}-{team_id_b}"

        # Score
        score_a = data.get("ScoreA")
        score_b = data.get("ScoreB")

        # Date formatting
        date_raw = data.get("Date")
        date = "-".join(date_raw.split("/")[::-1]) if date_raw else None

        # construct df row from api response
        row = {
            "game_id": game_id,
            "game": game,
            "date": date,
            "round": int(round_),
            "phase": phase.upper(),
            "season_code": season,
            "score_a": score_a,
            "score_b": score_b,
            "team_a": team_a,
            "team_b": team_b,
            "team_id_a": team_id_a,
            "team_id_b": team_id_b,
            "coach_a": data.get("CoachA"),
            "coach_b": data.get("CoachB"),
            "game_time": data.get("GameTime"),
            "remaining_partial_time": data.get("RemainingPartialTime"),
            "referee_1": data.get("Referee1"),
            "referee_2": data.get("Referee2"),
            "referee_3": data.get("Referee3"),
            "stadium": data.get("Stadium"),
            "capacity": data.get("Capacity"),
            "w_id": data.get("wid"),
            "fouls_a": data.get("FoultsA"),
            "fouls_b": data.get("FoultsB"),
            "timeouts_a": data.get("TimeoutsA"),
            "timeouts_b": data.get("TimeoutsB"),
        }

        # 🧠 Quarter scores (from Boxscore usually, but fallback if present)
        for i in range(1, 5):
            row[f"score_quarter_{i}_a"] = data.get(f"ScoreQuarter{i}A")
            row[f"score_quarter_{i}_b"] = data.get(f"ScoreQuarter{i}B")

        # Extra time (initialize as None)
        for i in range(1, 5):
            row[f"score_extra_time_{i}_a"] = None
            row[f"score_extra_time_{i}_b"] = None

        self.games_df_list.append(row)

    def process_comparison(self, data, season, gamecode, phase, round_):
        # unique game_id
        game_id = f"{season}_{str(gamecode).zfill(3)}"

        # construct df row from api response
        row = {
            "game_id": game_id,
            "round": int(round_),
            "phase": phase.upper(),
            "season_code": season,
            "defensive_rebounds_a": data.get("DefensiveReboundsA"),
            "offensive_rebounds_a": data.get("OffensiveReboundsA"),
            "defensive_rebounds_b": data.get("DefensiveReboundsB"),
            "offensive_rebounds_b": data.get("OffensiveReboundsB"),
            "turnovers_starters_a": data.get("TurnoversStartersA"),
            "turnovers_bench_a": data.get("TurnoversBenchA"),
            "turnovers_starters_b": data.get("TurnoversStartersB"),
            "turnovers_bench_b": data.get("TurnoversBenchB"),
            "steals_starters_a": data.get("StealsStartersA"),
            "steals_bench_a": data.get("StealsBenchA"),
            "steals_starters_b": data.get("StealsStartersB"),
            "steals_bench_b": data.get("StealsBenchB"),
            "assists_starters_a": data.get("AssistsStartersA"),
            "assists_bench_a": data.get("AssistsBenchA"),
            "assists_starters_b": data.get("AssistsStartersB"),
            "assists_bench_b": data.get("AssistsBenchB"),
            "points_starters_a": data.get("PointsStartersA"),
            "points_bench_a": data.get("PointsBenchA"),
            "points_starters_b": data.get("PointsStartersB"),
            "points_bench_b": data.get("PointsBenchB"),
            "max_a": data.get("maxA"),
            "max_b": data.get("maxB"),
            "max_lead_a": data.get("maxLeadA"),
            "max_lead_b": data.get("maxLeadB"),
        }

        self.comparison_df_list.append(row)

    def process_boxscore(self, data, season, gamecode, phase, round_, date):
        # extract teams
        team_id_a = data["ByQuarter"][0]["Team"]
        team_id_b = data["ByQuarter"][1]["Team"]
        game = f"{team_id_a}-{team_id_b}"

        for team_block in data["Stats"]:
            team_name = team_block["Team"]
            team_code = team_block["PlayersStats"][0]["Team"]  # e.g. IST / TEL

            # create row for each player, featuring his stats
            for player in team_block["PlayersStats"]:

                player_id = player["Player_ID"].strip()
                team_id = player["Team"]

                # unique game_id
                game_id = f"{season}_{str(gamecode).zfill(3)}"

                # Construct unique game_player_id
                game_player_id = f"{game_id}_{player_id}"

                row = {
                    "game_player_id": game_player_id,
                    "game_id": game_id,
                    "game": game,
                    "round": int(round_),
                    "phase": phase.upper(),
                    "season_code": season,
                    "player_id": player_id,
                    "is_starter": float(player["IsStarter"]),
                    "is_playing": float(player["IsPlaying"]),
                    "team_id": team_id,
                    "dorsal": player["Dorsal"],
                    "player": player["Player"],
                    "minutes": None if player["Minutes"] == "DNP" else player["Minutes"],
                    "points": player["Points"],
                    "two_points_made": player["FieldGoalsMade2"],
                    "two_points_attempted": player["FieldGoalsAttempted2"],
                    "three_points_made": player["FieldGoalsMade3"],
                    "three_points_attempted": player["FieldGoalsAttempted3"],
                    "free_throws_made": player["FreeThrowsMade"],
                    "free_throws_attempted": player["FreeThrowsAttempted"],
                    "offensive_rebounds": player["OffensiveRebounds"],
                    "defensive_rebounds": player["DefensiveRebounds"],
                    "total_rebounds": player["TotalRebounds"],
                    "assists": player["Assistances"],
                    "steals": player["Steals"],
                    "turnovers": player["Turnovers"],
                    "blocks_favour": player["BlocksFavour"],
                    "blocks_against": player["BlocksAgainst"],
                    "fouls_committed": player["FoulsCommited"],
                    "fouls_received": player["FoulsReceived"],
                    "valuation": player["Valuation"],
                    "plus_minus": player["Plusminus"],
                }

                self.players_df_list.append(row)

    def build_final_tables(self):
        return {
            "box_score": pd.DataFrame(self.players_df_list) if self.players_df_list else None,
            "header": pd.DataFrame(self.games_df_list) if self.games_df_list else None,
            "comparison": pd.DataFrame(self.comparison_df_list) if self.comparison_df_list else None,
        }
