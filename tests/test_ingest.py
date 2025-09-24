import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile
import pandas as pd

# Import your module
from ingest_data import ingest


class BaseTmpDirs(unittest.TestCase):
    """Create temp RAW_DIR/PROC_DIR for each test so we don't touch real files."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        ingest.RAW_DIR = base / "raw"
        ingest.PROC_DIR = base / "processed"
        ingest.RAW_DIR.mkdir(parents=True, exist_ok=True)
        ingest.PROC_DIR.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.tmp.cleanup()


class TestBootstrapAndFixtures(BaseTmpDirs):
    @patch("ingest_data.ingest._get")
    def test_fetch_bootstrap_saves_csvs(self, mock_get):
        mock_get.return_value = {
            "elements": [{"id": 1, "first_name": "A", "second_name": "B", "team": 10, "now_cost": 50, "total_points": 12}],
            "teams": [{"id": 10, "name": "TeamX", "strength_overall_home": 3, "strength_overall_away": 3}],
            "events": [{"id": 1, "finished": True, "is_current": False}, {"id": 2, "finished": False, "is_current": True}],
        }
        data = ingest.fetch_bootstrap(save=True)
        self.assertIn("elements", data)
        self.assertTrue((ingest.RAW_DIR / "bootstrap_players.csv").exists())
        self.assertTrue((ingest.RAW_DIR / "bootstrap_teams.csv").exists())
        self.assertTrue((ingest.RAW_DIR / "bootstrap_events.csv").exists())

    @patch("ingest_data.ingest._get")
    def test_fetch_fixtures_saves_csv(self, mock_get):
        mock_get.return_value = [
            {"event": 1, "team_h": 10, "team_a": 11, "team_h_difficulty": 2, "team_a_difficulty": 4},
            {"event": 2, "team_h": 11, "team_a": 10, "team_h_difficulty": 3, "team_a_difficulty": 2},
        ]
        df = ingest.fetch_fixtures(save=True)
        self.assertFalse(df.empty)
        self.assertTrue((ingest.RAW_DIR / "fixtures.csv").exists())


class TestEventLiveAndPlayerSummary(BaseTmpDirs):
    @patch("ingest_data.ingest._get")
    def test_fetch_event_live(self, mock_get):
        mock_get.return_value = {
            "elements": [
                {"id": 1, "stats": {"minutes": 90, "total_points": 10}},
                {"id": 2, "stats": {"minutes": 30, "total_points": 2}},
            ]
        }
        df = ingest.fetch_event_live(5, save=True)
        self.assertEqual(set(df.columns), {"player_id", "round", "minutes", "total_points"})
        self.assertEqual(df["round"].nunique(), 1)
        self.assertTrue((ingest.RAW_DIR / "gw_05_live.csv").exists())

    @patch("ingest_data.ingest._get")
    def test_fetch_player_summary(self, mock_get):
        mock_get.return_value = {
            "history": [{"round": 1, "minutes": 90, "total_points": 8}],
            "history_past": [{"season_name": "2022/23", "total_points": 200}],
        }
        hist, past = ingest.fetch_player_summary(123)
        self.assertIn("player_id", hist.columns)
        self.assertIn("player_id", past.columns)
        self.assertEqual(hist.loc[0, "player_id"], 123)
        self.assertEqual(past.loc[0, "player_id"], 123)


class TestAllPlayersHistories(BaseTmpDirs):
    @patch("ingest_data.ingest.fetch_player_summary")
    @patch("ingest_data.ingest.fetch_bootstrap")
    @patch("ingest_data.ingest.time.sleep", lambda *_: None)  # no delay
    def test_fetch_all_players_histories(self, mock_bootstrap, mock_summary):
        # Mock bootstrap to return two players
        mock_bootstrap.return_value = {"elements": [{"id": 101}, {"id": 102}]}
        # Mock per-player summaries
        h1 = pd.DataFrame([{"round": 1, "minutes": 90, "total_points": 7}]); h1["player_id"] = 101
        p1 = pd.DataFrame([{"season_name": "2022/23", "total_points": 180}]); p1["player_id"] = 101
        h2 = pd.DataFrame([{"round": 1, "minutes": 10, "total_points": 1}]); h2["player_id"] = 102
        p2 = pd.DataFrame([{"season_name": "2022/23", "total_points": 20}]);  p2["player_id"] = 102
        mock_summary.side_effect = [(h1, p1), (h2, p2)]

        gw_df, seasons_df = ingest.fetch_all_players_histories(sleep=0)
        self.assertEqual(len(gw_df), 2)
        self.assertEqual(len(seasons_df), 2)
        self.assertTrue((ingest.PROC_DIR / "gameweeks_current_season.csv").exists())
        self.assertTrue((ingest.PROC_DIR / "seasons_aggregates.csv").exists())


class TestBuildMergedDataset(BaseTmpDirs):
    def _write_minimal_bootstrap(self):
        # players
        pd.DataFrame([
            {"id": 201, "first_name": "Mo", "second_name": "Salah", "team": 10, "now_cost": 125, "total_points": 10, "selected_by_percent": "45.0"}
        ]).to_csv(ingest.RAW_DIR / "bootstrap_players.csv", index=False)

        # teams
        pd.DataFrame([
            {"id": 10, "name": "Liverpool", "strength_overall_home": 4, "strength_overall_away": 4},
            {"id": 11, "name": "Man City",  "strength_overall_home": 5, "strength_overall_away": 5},
        ]).to_csv(ingest.RAW_DIR / "bootstrap_teams.csv", index=False)

        # fixtures (GW 1: LIV vs MCI; GW 2: MCI vs LIV)
        pd.DataFrame([
            {"event": 1, "team_h": 10, "team_a": 11, "team_h_difficulty": 3, "team_a_difficulty": 4},
            {"event": 2, "team_h": 11, "team_a": 10, "team_h_difficulty": 4, "team_a_difficulty": 3},
        ]).to_csv(ingest.RAW_DIR / "fixtures.csv", index=False)

        # gameweeks_current_season (one row for Salah in GW1 & GW2)
        pd.DataFrame([
            {"player_id": 201, "round": 1, "minutes": 90, "goals_scored": 1, "assists": 0, "clean_sheets": 0, "total_points": 8,
             "transfers_in": 1000, "transfers_out": 200, "selected_by_percent": "45.0"},
            {"player_id": 201, "round": 2, "minutes": 85, "goals_scored": 0, "assists": 1, "clean_sheets": 0, "total_points": 5,
             "transfers_in": 800, "transfers_out": 300, "selected_by_percent": "46.0"},
        ]).to_csv(ingest.PROC_DIR / "gameweeks_current_season.csv", index=False)

    def test_build_merged_dataset(self):
        self._write_minimal_bootstrap()
        merged = ingest.build_merged_dataset()

        # Basic shape & save
        self.assertFalse(merged.empty)
        self.assertTrue((ingest.PROC_DIR / "merged_gameweeks.csv").exists())

        # Required columns present
        required = {"player_id","team_name","round","total_points","opponent_team","opponent_difficulty","was_home"}
        self.assertTrue(required.issubset(set(merged.columns)))

        # Check GW1 merge logic: team=10 was home vs 11 with difficulty 3
        row_gw1 = merged.loc[merged["round"] == 1].iloc[0]
        self.assertEqual(row_gw1["team_name"], "Liverpool")
        self.assertEqual(int(row_gw1["opponent_team"]), 11)
        self.assertEqual(int(row_gw1["opponent_difficulty"]), 4)  # opponent difficulty = other side's difficulty
        self.assertEqual(bool(row_gw1["was_home"]), True)

        # Check GW2: team=10 away vs 11; difficulty from team_h_difficulty of opponent = 4
        row_gw2 = merged.loc[merged["round"] == 2].iloc[0]
        self.assertEqual(int(row_gw2["opponent_team"]), 11)
        self.assertEqual(bool(row_gw2["was_home"]), False)


if __name__ == "__main__":
    unittest.main()
