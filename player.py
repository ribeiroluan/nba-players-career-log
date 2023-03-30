from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelogs
from nba_api.stats.endpoints import commonplayerinfo
import requests
import pandas as pd
import backoff
import time
from datetime import datetime
import os
import glob

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class PlayerCareer():
    
    def __init__(self, player_full_name:str, season_type:str) -> None:
        self.player_full_name = player_full_name
        self.season_type = season_type

    def _check_if_player_exists(self) -> bool:
        if players.find_players_by_full_name(self.player_full_name) == []:
            return False
        else:
            return True
        
    def _check_if_season_type_exists(self) -> bool:
        if self.season_type in ('Regular Season', 'Playoffs', 'Pre Season'):
            return True
        else:
            return False

    @property
    def player_id(self) -> int:
        """Get player id"""
        player = players.find_players_by_full_name(self.player_full_name)
        return player[0]["id"] 

    @property
    def player_first_year(self) -> int:
        """Get player first playing year"""
        player_info = commonplayerinfo.CommonPlayerInfo(player_id = self.player_id)
        first_year = player_info.get_data_frames()[0].iloc[0]["FROM_YEAR"]
        return first_year
    
    @property
    def player_last_year(self) -> int:
        """Get player last playing year"""
        player_info = commonplayerinfo.CommonPlayerInfo(player_id = self.player_id)
        last_year = player_info.get_data_frames()[0].iloc[0]["TO_YEAR"]
        return last_year
    
    def _adjust_season(self, year:int) -> str:
        """Transforms an year in an NBA season format: eg 2021 becomes 2021-22"""
        return str(year)+'-'+str(year+1)[2:]
    
    @backoff.on_exception(backoff.expo, (requests.exceptions.Timeout, requests.exceptions.ConnectionError), max_tries=20)
    def get_career(self) -> pd.DataFrame:
        """Get player career log"""
        
        if self._check_if_player_exists() and self._check_if_season_type_exists():
            
            year = self.player_first_year
            career_dict = []

            while year <= self.player_last_year:
                time.sleep(15) #avoiding timeouts
                season_log = playergamelogs.PlayerGameLogs(
                        player_id_nullable = self.player_id, 
                        season_nullable = self._adjust_season(year),
                        season_type_nullable=self.season_type
                        ).get_data_frames()[0]
                logger.info(f"Getting data for {self.player_full_name} for the {self._adjust_season(year)} {self.season_type}")
                career_dict.append(season_log)
                year += 1

            career = pd.concat(career_dict, ignore_index=True)
            return career
        
        else:
            logger.error(f"Player {self.player_full_name} does not exist or {self.season_type} is not available")


class DataCleaner():
    
    def __init__(self, data:pd.DataFrame, season_type:str):
        self.data = data
        self.season_type = season_type

    def _sort_values(self, data:pd.DataFrame):
        return data.sort_values('GAME_DATE', ascending = True, ignore_index=True)
    
    def _fix_date_format(self, data:pd.DataFrame):
        data['GAME_DATE'] = pd.to_datetime(data['GAME_DATE'], format="%Y-%m-%dT%H:%M:%S")
        return data
     
    def _add_season_type_column(self, data:pd.DataFrame):
        data['SEASON_TYPE'] = self.season_type
        return data

    def _remove_unwanted_columns(self, data:pd.DataFrame):
        for column in data.columns:
            if "RANK" in column or column in ['WNBA_FANTASY_PTS', 'VIDEO_AVAILABLE_FLAG']:
                data.drop(column, axis=1, inplace=True)
        return data
    
    def clean(self):
        return self._remove_unwanted_columns(self._add_season_type_column(self._fix_date_format(self._sort_values(self.data))))

class DataWriter():

    def __init__(self, player:PlayerCareer) -> None:
        self.player = player
        self.data = player.get_career()
    
    def _get_filename(self):
        """Get output file name"""
        return f"{(self.player.player_full_name.lower()).replace(' ', '')}-{self.player.season_type.lower().replace(' ', '')}-{datetime.today().strftime('%Y%m%d')}"
    
    def _clean_data(self):
        return DataCleaner(self.data, self.player.season_type).clean()
        
    def write(self) -> None:
        """Write career log to csv"""
        self._clean_data().to_csv("tmp/"+self._get_filename()+'.csv', index=False)
        logger.info(f"Career data wrote to {self._get_filename() + '.csv'} successfully")

class CleanFolder():

    def clean_folder(self):
        """Clean temp folder"""
        files = glob.glob("tmp/*.csv")
        for f in files:
            os.remove(f)