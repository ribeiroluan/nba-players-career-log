import pytest
from unittest.mock import patch
from airflow.code.player import PlayerCareer, DataWriter
import datetime


#Unit tests are not exhaustive
class TestPlayerCareer:
    
    @patch(target="nba_api.stats.static.players.find_players_by_full_name", return_value=["Player data 1", "Player data 2"])
    def test_check_if_player_exists(self, mock):
        player = PlayerCareer(player_full_name="Valid player name", season_type="Valid season type")
        actual = player._check_if_player_exists()
        expected = True
        assert actual == expected

    @pytest.mark.parametrize(
            "season_type, expected",
            [
                ("Regular Season", True), 
                ("Playoffs", True), 
                ("Pre Season", True),
                ("G League", False),
            ]
        )
    def test_check_if_season_type_exists(self, season_type, expected):
        player = PlayerCareer(player_full_name="Valid player name", season_type=season_type)
        actual = player._check_if_season_type_exists()
        assert actual  == expected

    @patch(target="nba_api.stats.static.players.find_players_by_full_name", return_value=[{"id":1234}])
    def test_player_id(self, mock):
        player = PlayerCareer(player_full_name="Valid player name", season_type="Valid season type")
        actual = player.player_id
        expected = 1234
        assert actual == expected

    @pytest.mark.parametrize(
            "year, expected",
            [
                (2021, "2021-22"), 
                (2000, "2000-01"), 
                (3007, "3007-08"),
            ]
        )
    def test_adjust_season(self, year, expected):
        player = PlayerCareer(player_full_name="Valid player name", season_type="Valid season type")
        actual = player._adjust_season(year)
        assert actual == expected

class TestDataWriter:
    
    def test_get_filename(self):
        player = PlayerCareer(player_full_name="Valid player name", season_type="Valid season type")
        actual = DataWriter(player=player)._get_filename()
        expected = f"validplayername-validseasontype-{datetime.datetime.today().strftime('%Y%m%d')}"
        assert actual == expected