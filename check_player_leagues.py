
import requests
import sys

url = "https://rwtips-r943.onrender.com/api/rw-matches"

print(f"Fetching from {url}...")
try:
    r = requests.get(url, timeout=30)
    data = r.json()
    matches = data if isinstance(data, list) else data.get('partidas', [])
    
    target_player = "fantazer" 
    
    player_matches = []
    
    print(f"Analyzing {len(matches)} total matches...")

    for m in matches:
        h = m.get('homeTeam', '') or m.get('homeClub', '')
        a = m.get('awayTeam', '') or m.get('awayClub', '')
        
        # Check against target player
        if target_player.lower() in h.lower() or target_player.lower() in a.lower():
            player_matches.append(m)
            
    print(f"Found {len(player_matches)} matches for {target_player}")

    distinct_leagues = set()
    matches_with_league = []

    for m in player_matches:
        league = m.get('league', 'N/A')
        distinct_leagues.add(league)
        matches_with_league.append(m)

    # Sort matches by date descending
    matches_with_league.sort(key=lambda x: x.get('matchTime', ''), reverse=True)

    print(f"{'Date':<25} | {'League':<40} | {'Score'}")
    print("-" * 80)

    for m in matches_with_league[:20]:
        date = m.get('matchTime', 'N/A')
        league = m.get('league', 'N/A')
        score = f"{m.get('homeFT')}-{m.get('awayFT')}"
        print(f"{date:<25} | {league:<40} | {score}")

    print("-" * 80)
    print("Distinct Leagues Found:")
    for l in sorted(distinct_leagues):
        print(f"- {l}")

except Exception as e:
    print(f"Error: {e}")
