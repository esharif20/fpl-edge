import unittest
from src.ingesting_data.ingest import fetch_players   


class TestIngest(unittest.TestCase):
    def test_fetch_players_not_empty(self):
        df = fetch_players()
        self.assertFalse(df.empty)

    def test_fetch_players_has_columns(self):
        df = fetch_players()
        expected_cols = {"id", "first_name", "second_name", "team", "now_cost", "total_points"}
        self.assertTrue(expected_cols.issubset(df.columns))

if __name__ == "__main__":
    unittest.main()
