"""
The Impact Board — Daily Update Pipeline
==========================================
python backend/ingest/daily_update.py

Runs the full nightly update in sequence:
  1. fetch_season.py       — re-fetch all season aggregate data
  2. fetch_new_games.py    — incremental PBP for new games only
  3. compute_metrics.py    — recompute all derived metrics

Designed to run as a Railway cron job at 3am ET daily.
Can also be run manually: python backend/ingest/daily_update.py
"""

import os
import sys
import subprocess
from datetime import datetime

def run(script, label):
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting: {label}")
    print(f"{'='*60}")

    result = subprocess.run(
        [sys.executable, script],
        cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        capture_output=False,
    )

    if result.returncode != 0:
        print(f"\n❌ {label} failed with exit code {result.returncode}")
        return False

    print(f"\n✅ {label} complete")
    return True


def main():
    print(f"\n{'='*60}")
    print(f"THE IMPACT BOARD — Daily Update")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    base = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'ingest'
    )

    steps = [
        (os.path.join(base, 'fetch_season.py'),       'Season data fetch'),
        (os.path.join(base, 'fetch_new_pbp_stats.py'),'Incremental PBP stats'),
        (os.path.join(base, 'compute_metrics.py'),     'Metrics computation'),
    ]

    for script, label in steps:
        if not os.path.exists(script):
            print(f"⚠️  Skipping {label} — {script} not found")
            continue
        success = run(script, label)
        if not success:
            print(f"\n❌ Pipeline stopped at: {label}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"✅ Daily update complete")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()