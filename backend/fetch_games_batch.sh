#!/bin/bash
# Batch fetch game data for historical seasons.
# Run from the project root: bash backend/fetch_games_batch.sh

SEASONS=(
  "2024-25"
  "2023-24"
  "2022-23"
  "2021-22"
  "2020-21"
  "2019-20"
  "2018-19"
  "2017-18"
  "2016-17"
  "2015-16"
  "2014-15"
  "2013-14"
  "2012-13"
  "2011-12"
  "2010-11"
)

DELAY=20  # seconds between seasons (avoids NBA API rate limits)

for season in "${SEASONS[@]}"; do
  echo ""
  echo "════════════════════════════════════"
  echo "  Season: $season"
  echo "════════════════════════════════════"
  python backend/fetch_games.py --season "$season" --all
  if [ "$season" != "${SEASONS[${#SEASONS[@]}-1]}" ]; then
    echo "  ⏳ Waiting ${DELAY}s before next season..."
    sleep $DELAY
  fi
done

echo ""
echo "✅ Batch complete."
