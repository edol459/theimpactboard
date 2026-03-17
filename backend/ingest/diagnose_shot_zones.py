"""
Diagnose shot location endpoint columns
python backend/ingest/diagnose_shot_zones.py
"""
import time
from dotenv import load_dotenv
load_dotenv()

def try_import(name):
    try:
        import importlib
        mod = importlib.import_module("nba_api.stats.endpoints")
        return getattr(mod, name)
    except (ImportError, AttributeError):
        return None

LeagueDashPlayerShotLocations = try_import("LeagueDashPlayerShotLocations")

print("Fetching shot locations...")
time.sleep(1.5)
try:
    ep  = LeagueDashPlayerShotLocations(
        season='2024-25',
        season_type_all_star='Regular Season',
        distance_range='By Zone',
    )
    dfs = ep.get_data_frames()
    print(f"{len(dfs)} result sets")
    for i, df in enumerate(dfs):
        print(f"\nRS{i}: {len(df)} rows")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        if len(df) > 0:
            print(f"\nSample row:")
            print(df.iloc[0].to_string())
except Exception as e:
    print(f"Error: {e}")