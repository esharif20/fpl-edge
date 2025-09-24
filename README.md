# fpl-edge
## Directory structure
```bash
fpl-edge/
├─ src/
│  ├─ ingest_data/
│  │  ├─ __init__.py
│  │  └─ ingest.py
│  ├─ fpl_data/
│  │  ├─ raw/          # raw API snapshots
│  │  └─ processed/    # merged, model-ready datasets
│  └─ ...
├─ notebooks/
├─ tests/
├─ venv/
└─ README.md

```

## Setup (first time)

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install -r requirements.txt

```

## Ingestion commands

```bash
PYTHONPATH=src python -m ingest_data.ingest
```
#### 0utputs:
```
src/fpl_data/raw/bootstrap_players.csv

src/fpl_data/raw/bootstrap_teams.csv

src/fpl_data/raw/bootstrap_events.csv

src/fpl_data/raw/fixtures.csv

src/fpl_data/processed/gameweeks_current_season.csv

src/fpl_data/processed/seasons_aggregates.csv

src/fpl_data/processed/merged_gameweeks.csv
```

## Useful API calls (manual testing)
```bash
# Bootstrap (players, teams, events)
curl https://fantasy.premierleague.com/api/bootstrap-static/

# Fixtures
curl https://fantasy.premierleague.com/api/fixtures/

# GW live stats (replace {gw})
curl https://fantasy.premierleague.com/api/event/{gw}/live/

# Per-player summary (replace {id})
curl https://fantasy.premierleague.com/api/element-summary/{id}/
```
## Testing
```bash
PYTHONPATH=src python -m unittest discover -s tests -p "test*.py"
```