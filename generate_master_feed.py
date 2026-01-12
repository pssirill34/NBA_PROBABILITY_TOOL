import requests
import pandas as pd
import time
import os
from datetime import date

import os # Ensure this is imported

# --- Configuration ---
# This tells Python: "Look for a variable named API_KEY in the system environment"
API_KEY = os.environ.get("API_KEY")

# Safety check to ensure the key loaded
if not API_KEY:
    raise ValueError("API Key not found! Check GitHub Secrets.")
BASE_URL = "https://api.balldontlie.io/v1"
OUTPUT_FILE = "nba_game_logs_2024_25.csv"

# Fetch 2024 (History) + 2025 (Current) for robust sample size
SEASONS = [2024, 2025]

HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}


def get_todays_active_rosters():
    """
    1. Scans today's games.
    2. Fetches the OFFICIAL roster for those teams.
    This eliminates 'cut' players and 10-day contracts from last year.
    """
    print(f"📅 Scanning Schedule for {date.today()}...")
    try:
        # 1. Get Games
        params = {"dates[]": [date.today().isoformat()]}
        r = requests.get(f"{BASE_URL}/games", headers=HEADERS, params=params)
        games = r.json().get('data', [])

        if not games:
            print("   ⚠️ No games found today.")
            return []

        team_ids = set()
        for g in games:
            team_ids.add(g['home_team']['id'])
            team_ids.add(g['visitor_team']['id'])

        # 2. Get Rosters
        print(f"   Fetching active rosters for {len(team_ids)} teams...")
        active_ids = []
        for tid in team_ids:
            cursor = None
            while True:
                try:
                    p = {"team_ids[]": [tid], "per_page": 100}
                    if cursor: p['cursor'] = cursor
                    r = requests.get(f"{BASE_URL}/players", headers=HEADERS, params=p)
                    d = r.json()
                    for player in d.get('data', []):
                        active_ids.append(player['id'])
                    cursor = d.get('meta', {}).get('next_cursor')
                    if not cursor: break
                    time.sleep(0.1)
                except:
                    break

        unique_ids = list(set(active_ids))
        print(f"   ✅ Identified {len(unique_ids)} active players.")
        return unique_ids

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []


def fetch_game_logs(player_ids):
    """
    Fetches ALL game logs (Standard & Advanced) for the target list.
    """
    print(f"📊 Building Data Lake (2024-2025)...")

    # Chunking to respect API limits
    chunk_size = 50
    std_logs = []
    adv_logs = []

    total_chunks = (len(player_ids) // chunk_size) + 1

    for i in range(0, len(player_ids), chunk_size):
        chunk = player_ids[i:i + chunk_size]
        print(f"   Processing Chunk {i // chunk_size + 1}/{total_chunks}...", end="\r")

        # A. Fetch Standard Stats
        cursor = None
        while True:
            try:
                params = {"seasons[]": SEASONS, "player_ids[]": chunk, "per_page": 100}
                if cursor: params['cursor'] = cursor
                r = requests.get(f"{BASE_URL}/stats", headers=HEADERS, params=params)
                if r.status_code == 429: time.sleep(2); continue
                d = r.json()
                std_logs.extend(d.get('data', []))
                cursor = d.get('meta', {}).get('next_cursor')
                if not cursor: break
                time.sleep(0.12)
            except:
                break

        # B. Fetch Advanced Stats
        cursor = None
        while True:
            try:
                params = {"seasons[]": SEASONS, "player_ids[]": chunk, "per_page": 100}
                if cursor: params['cursor'] = cursor
                r = requests.get(f"{BASE_URL}/stats/advanced", headers=HEADERS, params=params)
                if r.status_code == 429: time.sleep(2); continue
                d = r.json()
                adv_logs.extend(d.get('data', []))
                cursor = d.get('meta', {}).get('next_cursor')
                if not cursor: break
                time.sleep(0.12)
            except:
                break

    print(f"\n   ✅ Fetched {len(std_logs)} standard and {len(adv_logs)} advanced records.")
    return std_logs, adv_logs


def merge_and_export(std_logs, adv_logs):
    print("⚙️  Merging Datasets...")

    # Index Advanced Logs by (PlayerID, GameID)
    adv_map = {}
    for a in adv_logs:
        key = (a['player']['id'], a['game']['id'])
        adv_map[key] = a

    rows = []
    for s in std_logs:
        # Skip DNPs
        if s['min'] in [None, "0", "00", "0:00", ""]: continue

        # Parse Min
        try:
            m = str(s['min'])
            mins = int(m.split(":")[0]) + int(m.split(":")[1]) / 60 if ":" in m else float(m)
        except:
            mins = 0.0

        # Identifiers
        pid = s['player']['id']
        gid = s['game']['id']

        # Get Advanced Data
        adv = adv_map.get((pid, gid), {})

        # Context
        game = s['game']
        team = s['team']
        is_home = (team['id'] == game['home_team_id'])
        opp_id = game['visitor_team_id'] if is_home else game['home_team_id']

        # Basic Stats
        pts = s.get('pts', 0) or 0
        reb = s.get('reb', 0) or 0
        ast = s.get('ast', 0) or 0

        row = {
            "Date": game['date'][:10],
            "Season": 2025 if game['date'] > '2024-09-01' else 2024,
            "Player": f"{s['player']['first_name']} {s['player']['last_name']}",
            "Team": team['abbreviation'],
            "Loc": "Home" if is_home else "Away",
            "Opp_ID": opp_id,

            # Metrics
            "MIN": round(mins, 2),
            "PTS": pts,
            "REB": reb,
            "AST": ast,
            "STL": s.get('stl', 0) or 0,
            "BLK": s.get('blk', 0) or 0,
            "TOV": s.get('turnover', 0) or 0,

            # Combos
            "PRA": pts + reb + ast,
            "PR": pts + reb,
            "PA": pts + ast,
            "Stocks": (s.get('stl', 0) or 0) + (s.get('blk', 0) or 0),

            # Advanced
            "Usage": round((adv.get('usage_percentage') or 0) * 100, 1),
            "DefRtg": adv.get('defensive_rating'),
            "NetRtg": adv.get('net_rating'),
            "Pace": adv.get('pace')
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df.sort_values(['Date', 'Player'], ascending=[False, True], inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ SUCCESS! Saved {len(df)} rows to {OUTPUT_FILE}")


def main():
    # 1. Census
    ids = get_todays_active_rosters()
    if not ids: return

    # 2. Fetch History
    std, adv = fetch_game_logs(ids)

    # 3. Merge
    merge_and_export(std, adv)


if __name__ == "__main__":

    main()
