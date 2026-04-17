"""
learn_credit_weights.py — Learn Optimal Credit Assignment Weights for PVA
==========================================================================
Searches for credit weight parameters that produce player ratings most
predictive of holdout possession outcomes.

Parameters learned:
  alpha  — shooter's share of credit on assisted made FGs (assister gets 1-alpha)
            currently hardcoded at 0.50 in compute_pva.py
  beta   — fraction of credit that stays with the primary actor(s) vs.
            spreading to the other 3-4 offensive players on court
            currently hardcoded at 1.0 (all credit to shooter/assister, none to others)

A low beta means the data wants credit spread to unobservable contributors
(screeners, cutters, spacers). The magnitude of (1 - beta) quantifies how much
of basketball value is invisible in play-by-play data.

Defensive credit remains 1/5 per defender (no parameter yet).

Usage:
    python backend/ingest/learn_credit_weights.py --season 2024-25
    python backend/ingest/learn_credit_weights.py --season 2024-25 --test-fraction 0.25
"""

import argparse
import logging
import os
from collections import defaultdict

import numpy as np
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from scipy.optimize import minimize

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


# ── Data loading ───────────────────────────────────────────────────────────────

LOAD_SQL = """
WITH last_actor AS (
    SELECT DISTINCT ON (pe.possession_id)
        pe.possession_id,
        pe.player_id        AS shooter_id,
        pe.action_type      AS actor_action_type,
        pe.assist_player_id AS assister_id
    FROM possession_events pe
    JOIN possessions p ON p.id = pe.possession_id
    WHERE p.season = %s
      AND p.expected_points IS NOT NULL
      AND pe.action_type IN ('2pt', '3pt', 'turnover', 'freethrow')
      AND pe.player_id IS NOT NULL
      AND NOT (ABS(p.score_margin_offense) > 15 AND p.game_seconds_start > 3 * 720)
    ORDER BY pe.possession_id, pe.event_index DESC
),
off_lineups AS (
    SELECT possession_id, array_agg(player_id) AS off_players
    FROM possession_lineups
    WHERE side = 'offense'
    GROUP BY possession_id
),
def_lineups AS (
    SELECT possession_id, array_agg(player_id) AS def_players
    FROM possession_lineups
    WHERE side = 'defense'
    GROUP BY possession_id
)
SELECT
    p.id                AS possession_id,
    p.game_id,
    p.points_scored,
    p.expected_points,
    p.points_scored - p.expected_points AS pva,
    p.end_reason,
    p.shot_zone,
    la.shooter_id,
    CASE WHEN la.actor_action_type IN ('2pt','3pt') AND la.assister_id IS NOT NULL
         THEN la.assister_id ELSE NULL END AS assister_id,
    ol.off_players,
    dl.def_players
FROM possessions p
JOIN last_actor   la ON la.possession_id = p.id
JOIN off_lineups  ol ON ol.possession_id = p.id
JOIN def_lineups  dl ON dl.possession_id = p.id
WHERE p.season = %s
  AND p.expected_points IS NOT NULL
  AND array_length(ol.off_players, 1) = 5
  AND array_length(dl.def_players, 1) = 5
ORDER BY p.game_id, p.id
"""


def load_possessions(season: str) -> list:
    log.info(f"Loading possessions for {season}...")
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(LOAD_SQL, (season, season))
    rows = cur.fetchall()
    cur.close(); conn.close()
    log.info(f"  Loaded {len(rows):,} possessions")
    return rows


def train_test_split(rows: list, test_fraction: float = 0.25):
    """Split by game_id. Last test_fraction of games (sorted by game_id) are holdout."""
    game_ids = sorted(set(r["game_id"] for r in rows))
    cutoff   = int(len(game_ids) * (1 - test_fraction))
    train_games = set(game_ids[:cutoff])
    test_games  = set(game_ids[cutoff:])
    train = [r for r in rows if r["game_id"] in train_games]
    test  = [r for r in rows if r["game_id"] in test_games]
    log.info(f"  Train: {len(train):,} possessions ({len(train_games)} games)")
    log.info(f"  Test:  {len(test):,}  possessions ({len(test_games)} games)")
    return train, test


# ── Credit computation ─────────────────────────────────────────────────────────

def compute_player_bases(train_rows: list) -> dict:
    """
    Precompute theta-independent base sums per player so that ratings for any
    (alpha, beta) can be computed in O(players) rather than O(possessions).

    For each player, stores:
      shooter_assisted   — sum of pva where player was shooter on an assisted made FG
      shooter_other      — sum of pva where player was shooter on anything else
      assister           — sum of pva where player was assister
      other_off          — sum of pva/n_others where player was a non-primary off player
      def_fixed          — sum of -pva/5 (defender credit, not parameterized)
      off_poss           — count of offensive possessions on court
      def_poss           — count of defensive possessions on court
    """
    bases = defaultdict(lambda: {
        "shooter_assisted": 0.0,
        "shooter_other":    0.0,
        "assister":         0.0,
        "other_off":        0.0,
        "def_fixed":        0.0,
        "off_poss":         0,
        "def_poss":         0,
    })

    for r in train_rows:
        pva        = float(r["pva"])
        shooter_id = r["shooter_id"]
        assister_id = r["assister_id"]
        off_players = list(r["off_players"])
        def_players = list(r["def_players"])
        is_assisted = (
            assister_id is not None
            and r["end_reason"] == "made_fg"
        )

        # ── Offensive players ──────────────────────────────────────────────────
        for pid in off_players:
            bases[pid]["off_poss"] += 1

        if shooter_id is not None:
            if is_assisted:
                # Primary pair: shooter + assister
                bases[shooter_id]["shooter_assisted"] += pva
                bases[assister_id]["assister"]        += pva
                others = [p for p in off_players
                          if p != shooter_id and p != assister_id]
            else:
                # Single primary actor
                bases[shooter_id]["shooter_other"] += pva
                others = [p for p in off_players if p != shooter_id]

            n = max(len(others), 1)
            for pid in others:
                bases[pid]["other_off"] += pva / n
        else:
            # No identifiable primary actor — spread evenly
            n = max(len(off_players), 1)
            for pid in off_players:
                bases[pid]["other_off"] += pva / n

        # ── Defensive players (fixed 1/5 each, no parameter) ──────────────────
        for pid in def_players:
            bases[pid]["def_fixed"] += -pva / 5.0
            bases[pid]["def_poss"]  += 1

    return bases


def compute_ratings(bases: dict, alpha: float, beta: float) -> dict:
    """
    Given precomputed bases and parameters (alpha, beta), return per-player
    credit-per-possession dicts with keys 'off' and 'def'.

    alpha  — shooter's share on assisted makes [0, 1]
    beta   — fraction of credit to primary actor(s) vs. spread to others [0, 1]
    """
    ratings = {}
    for pid, b in bases.items():
        off_credit = (
            alpha       * beta * b["shooter_assisted"]   # shooter on assisted make
            +             beta * b["shooter_other"]       # shooter on non-assisted
            + (1-alpha) * beta * b["assister"]            # assister on assisted make
            + (1-beta)        * b["other_off"]            # everyone else on offense
        )
        def_credit = b["def_fixed"]

        off_poss = b["off_poss"]
        def_poss = b["def_poss"]

        ratings[pid] = {
            "off": off_credit / off_poss if off_poss > 0 else 0.0,
            "def": def_credit / def_poss if def_poss > 0 else 0.0,
        }
    return ratings


# ── Evaluation ─────────────────────────────────────────────────────────────────

def evaluate(ratings: dict, test_rows: list) -> float:
    """
    Predict possession PVA from player ratings and return RMSE against actual PVA.

    predicted_pva = mean(5 off ratings) - mean(5 def ratings)

    Sign convention:
      off rating: positive → offense contributes above expected
      def rating: positive → defense holds offense below expected
    So subtracting def_rating from off_rating gives the net offensive outcome.
    """
    default = {"off": 0.0, "def": 0.0}
    predicted = np.array([
        np.mean([ratings.get(p, default)["off"] for p in r["off_players"]])
        - np.mean([ratings.get(p, default)["def"] for p in r["def_players"]])
        for r in test_rows
    ])
    actual = np.array([float(r["pva"]) for r in test_rows])
    return float(np.sqrt(np.mean((predicted - actual) ** 2)))


# ── Optimization ───────────────────────────────────────────────────────────────

def optimize(train_rows: list, test_rows: list):
    log.info("Precomputing player bases from train possessions...")
    bases = compute_player_bases(train_rows)
    log.info(f"  {len(bases)} unique players in train set")

    # Baseline: current hardcoded weights
    baseline = evaluate(compute_ratings(bases, alpha=0.5, beta=1.0), test_rows)
    log.info(f"Baseline RMSE  (alpha=0.50, beta=1.00): {baseline:.6f}")

    # Also check a few reference points
    for a, b, label in [
        (0.5, 0.5, "equal-spread"),
        (0.7, 1.0, "shooter-heavy"),
        (0.3, 1.0, "assister-heavy"),
        (0.5, 0.7, "30% to others"),
    ]:
        rmse = evaluate(compute_ratings(bases, a, b), test_rows)
        log.info(f"  alpha={a:.1f} beta={b:.1f} ({label:18s}): {rmse:.6f}")

    def objective(params):
        a, b = params
        return evaluate(compute_ratings(bases, a, b), test_rows)

    # Grid search for starting point
    log.info("Running grid search (9×9)...")
    best_rmse   = float("inf")
    best_params = (0.5, 1.0)
    for a in np.linspace(0.1, 0.9, 9):
        for b in np.linspace(0.2, 1.0, 9):
            rmse = objective([a, b])
            if rmse < best_rmse:
                best_rmse   = rmse
                best_params = (float(a), float(b))

    log.info(f"Grid best: alpha={best_params[0]:.2f}, beta={best_params[1]:.2f}, RMSE={best_rmse:.6f}")

    # Fine-tune with L-BFGS-B
    result = minimize(
        objective,
        x0=list(best_params),
        method="L-BFGS-B",
        bounds=[(0.01, 0.99), (0.01, 1.00)],
        options={"ftol": 1e-12, "gtol": 1e-10, "maxiter": 500},
    )
    alpha_opt, beta_opt = result.x
    rmse_opt = float(result.fun)

    improvement_pct = (baseline - rmse_opt) / baseline * 100

    log.info("")
    log.info("=" * 60)
    log.info("RESULTS")
    log.info("=" * 60)
    log.info(f"Baseline  alpha=0.50 beta=1.00  RMSE={baseline:.6f}")
    log.info(f"Optimized alpha={alpha_opt:.3f} beta={beta_opt:.3f}  RMSE={rmse_opt:.6f}")
    log.info(f"Improvement: {improvement_pct:.3f}%")
    log.info("")
    log.info("Interpretation:")
    log.info(f"  alpha = {alpha_opt:.3f}")
    log.info(f"    → shooter receives {alpha_opt*100:.1f}% of assisted-make credit")
    log.info(f"    → assister receives {(1-alpha_opt)*100:.1f}%")
    log.info(f"  beta  = {beta_opt:.3f}")
    log.info(f"    → {beta_opt*100:.1f}% of credit concentrates on primary actor(s)")
    log.info(f"    → {(1-beta_opt)*100:.1f}% spreads to the other offensive players on court")
    log.info(f"       (quantifies credit due to unobservable contributions: screens,")
    log.info(f"        spacing, off-ball cuts — things not in play-by-play)")
    log.info("")

    if improvement_pct < 0.01:
        log.info("NOTE: Minimal improvement over baseline. With one season of data,")
        log.info("the optimization signal may be too noisy to distinguish credit schemes.")
        log.info("Try collecting 2023-24 data for a more reliable result.")

    return alpha_opt, beta_opt, baseline, rmse_opt


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Learn optimal PVA credit assignment weights"
    )
    parser.add_argument("--season",        default="2024-25")
    parser.add_argument("--test-fraction", type=float, default=0.25,
                        help="Fraction of games to hold out for evaluation (default 0.25)")
    args = parser.parse_args()

    rows = load_possessions(args.season)
    train_rows, test_rows = train_test_split(rows, args.test_fraction)
    optimize(train_rows, test_rows)


if __name__ == "__main__":
    main()
