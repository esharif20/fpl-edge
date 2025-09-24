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

if __name__ == "__main__":
    df = fetch_players()
    df.to_csv("data/players.csv", index=False)
    print(f"Saved {len(df)} players to data/players.csv")
