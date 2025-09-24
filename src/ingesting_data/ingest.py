import os
import requests
import pandas as pd

FPL_BOOTSTRAP = "https://fantasy.premierleague.com/api/bootstrap-static/"

def fetch_players() -> pd.DataFrame:
    """Fetch all FPL player data into a DataFrame."""
    resp = requests.get(FPL_BOOTSTRAP)
    resp.raise_for_status()
    data = resp.json()
    players = pd.DataFrame(data["elements"])
    return players[["id", "first_name", "second_name", "team", "now_cost", "total_points"]]

import os

if __name__ == "__main__":
    df = fetch_players()
    data_dir = os.path.join("src", "fpl_data", "raw")
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, "players.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} players to {out_path}")
