import pytest
from unittest.mock import patch, mock_open
from player import PlayerCareer

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
class TestDataCleaner:
    pass

class TestDataWriter:
    pass

class TestCleanFolder:
    pass