import time
import requests
import pandas as pd
from pathlib import Path

BASE = "https://fantasy.premierleague.com/api/"

# --- Directories ---
SRC_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = SRC_DIR / "fpl_data" / "raw"
PROC_DIR = SRC_DIR / "fpl_data" / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

# --- Helper ---
def _get(url: str, timeout=30):
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# --- API Methods ---
def fetch_bootstrap(save=True):
    """Fetch global players, teams, and events (GW metadata)."""
    data = _get(BASE + "bootstrap-static/")
    if save:
        pd.DataFrame(data["elements"]).to_csv(RAW_DIR / "bootstrap_players.csv", index=False)
        pd.DataFrame(data["teams"]).to_csv(RAW_DIR / "bootstrap_teams.csv", index=False)
        pd.DataFrame(data["events"]).to_csv(RAW_DIR / "bootstrap_events.csv", index=False)
    return data

def fetch_fixtures(save=True):
    """Fetch all fixtures (past & future)."""
    fx = _get(BASE + "fixtures/")
    df = pd.DataFrame(fx)
    if save:
        df.to_csv(RAW_DIR / "fixtures.csv", index=False)
    return df

def fetch_event_live(gw: int, save=True):
    """Fetch all player stats for a given gameweek."""
    data = _get(BASE + f"event/{gw}/live/")
    rows = []
    for e in data.get("elements", []):
        pid = e["id"]
        stats = e.get("stats", {})
        stats["player_id"] = pid
        stats["round"] = gw
        rows.append(stats)
    df = pd.DataFrame(rows)
    if save:
        df.to_csv(RAW_DIR / f"gw_{gw:02d}_live.csv", index=False)
    return df

def fetch_player_summary(player_id: int):
    """Fetch one player’s current-season GW history + past season aggregates."""
    data = _get(BASE + f"element-summary/{player_id}/")
    hist = pd.DataFrame(data["history"])
    hist["player_id"] = player_id
    past = pd.DataFrame(data["history_past"])
    past["player_id"] = player_id
    return hist, past

def fetch_all_players_histories(sleep=0.25):
    """Loop all players, build current-season per-GW + past seasons aggregates."""
    players = pd.DataFrame(fetch_bootstrap(save=False)["elements"])
    all_gw, all_seasons = [], []
    for pid in players["id"]:
        try:
            h, p = fetch_player_summary(pid)
            all_gw.append(h)
            all_seasons.append(p)
            time.sleep(sleep)  # be polite
        except Exception as e:
            print(f"player {pid} failed: {e}")
    gw_df = pd.concat(all_gw, ignore_index=True) if all_gw else pd.DataFrame()
    seasons_df = pd.concat(all_seasons, ignore_index=True) if all_seasons else pd.DataFrame()
    gw_df.to_csv(PROC_DIR / "gameweeks_current_season.csv", index=False)
    seasons_df.to_csv(PROC_DIR / "seasons_aggregates.csv", index=False)
    return gw_df, seasons_df

def build_merged_dataset():
    # Load
    players  = pd.read_csv(RAW_DIR / "bootstrap_players.csv")
    teams    = pd.read_csv(RAW_DIR / "bootstrap_teams.csv")
    fixtures = pd.read_csv(RAW_DIR / "fixtures.csv")
    gw       = pd.read_csv(PROC_DIR / "gameweeks_current_season.csv")

    # --- Merge player metadata (adds 'team' id to gw) ---
    df = gw.merge(
        players,
        left_on="player_id",
        right_on="id",
        suffixes=("", "_player")
    )

    # --- Add team names/strengths (join team id) ---
    df = df.merge(
        teams[["id", "name", "strength_overall_home", "strength_overall_away"]],
        left_on="team", right_on="id", how="left", suffixes=("", "_team")
    ).rename(columns={"name": "team_name"}).drop(columns=["id_team"], errors="ignore")

    # --- Make fixtures LONG: one row per (event, team) with opponent + difficulty ---
    fx = fixtures[["event","team_h","team_a","team_h_difficulty","team_a_difficulty"]].copy()

    home = fx.rename(columns={
        "team_h":"team",
        "team_a":"opponent_team",
        "team_a_difficulty":"opponent_difficulty"
    }).assign(was_home=True)[["event","team","opponent_team","opponent_difficulty","was_home"]]

    away = fx.rename(columns={
        "team_a":"team",
        "team_h":"opponent_team",
        "team_h_difficulty":"opponent_difficulty"
    }).assign(was_home=False)[["event","team","opponent_team","opponent_difficulty","was_home"]]

    fx_long = pd.concat([home, away], ignore_index=True)

    # Ensure ints (avoid merge misses due to dtype)
    for c in ["event","team","opponent_team"]:
        fx_long[c] = pd.to_numeric(fx_long[c], errors="coerce").astype("Int64")

    if "team" in df.columns:
        df["team"] = pd.to_numeric(df["team"], errors="coerce").astype("Int64")
    if "round" in df.columns:
        df["round"] = pd.to_numeric(df["round"], errors="coerce").astype("Int64")

    # --- Single clean merge: (round,event) + team ---
    df = df.merge(
        fx_long,
        left_on=["round","team"],
        right_on=["event","team"],
        how="left"
    ).drop(columns=["event"], errors="ignore")

    # --- Select useful columns (keep if present) ---
    keep = [
        "player_id", "first_name", "second_name", "team_name", "round",
        "minutes", "goals_scored", "assists", "clean_sheets", "total_points",
        "now_cost", "transfers_in", "transfers_out", "selected_by_percent",
        "opponent_team", "opponent_difficulty", "was_home",
        "strength_overall_home", "strength_overall_away"
    ]
    cols = [c for c in keep if c in df.columns]
    merged = df[cols].copy()

    # Optional: tidy types
    if "selected_by_percent" in merged.columns:
        merged["selected_by_percent"] = pd.to_numeric(
            merged["selected_by_percent"].astype(str).str.replace("%","", regex=False),
            errors="coerce"
        )

    # Save
    out = PROC_DIR / "merged_gameweeks.csv"
    merged.to_csv(out, index=False)
    print(f"Saved merged dataset: {len(merged)} rows → {out}")
    return merged



# --- CLI Entrypoint ---
if __name__ == "__main__":
    print("Fetching bootstrap + fixtures...")
    fetch_bootstrap()
    fetch_fixtures()

    print("Fetching all players’ histories (this may take time)...")
    gw_df, seasons_df = fetch_all_players_histories()

    print("Building merged dataset...")
    merged = build_merged_dataset()

    print(f"Done! GW rows: {len(gw_df)}, season rows: {len(seasons_df)}, merged rows: {len(merged)}")