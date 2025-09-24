import unittest
import pandas as pd
from ingest_data.ingest import fetch_bootstrap

class TestIngest(unittest.TestCase):
    def test_fetch_players_not_empty(self):
        data = fetch_bootstrap(save=False)
        players = pd.DataFrame(data["elements"])
        self.assertFalse(players.empty)

    def test_fetch_players_has_columns(self):
        data = fetch_bootstrap(save=False)
        players = pd.DataFrame(data["elements"])
        expected = {"id", "first_name", "second_name", "team", "now_cost", "total_points"}
        self.assertTrue(expected.issubset(players.columns))

if __name__ == "__main__":
    unittest.main()
    fetch_fixtures()
    data = fetch_bootstrap(save=False)
    players = pd.DataFrame(data["elements"])
    histories = []
    for pid in players["id"].head(5):  # Test with first 5 players
        hist = fetch_player_history(pid, save=False)
        histories.append(hist)
    all_histories = pd.concat(histories, ignore_index=True)
    self.assertFalse(all_histories.empty)
    self.assertIn("player_id", all_histories.columns)
    self.assertIn("round", all_histories.columns)   