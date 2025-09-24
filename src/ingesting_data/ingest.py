import requests
import pandas as pd
from pathlib import Path

FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"

def fetch_players() -> pd.DataFrame:
    """Fetch all FPL player data into a DataFrame."""
    resp = requests.get(FPL_BOOTSTRAP, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    players = pd.DataFrame(data["elements"])
    return players[["id", "first_name", "second_name", "team", "now_cost", "total_points"]]

if __name__ == "__main__":
    SRC_DIR = Path(__file__).resolve().parents[1]
    RAW_DIR = SRC_DIR / "fpl_data" / "raw"
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    df = fetch_players()
    out_path = RAW_DIR / "players.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {len(df)} players to {out_path}")
