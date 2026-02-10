
import requests
from collections import Counter

url = "https://rwtips-r943.onrender.com/api/rw-matches"
print(f"Fetching from {url}...")
try:
    r = requests.get(url, timeout=30)
    data = r.json()
    matches = data if isinstance(data, list) else data.get('partidas', [])
    
    target_player = "fantazer"
    player_matches = []
    
    for m in matches:
        h = m.get('homeTeam', '') or m.get('homeClub', '')
        a = m.get('awayTeam', '') or m.get('awayClub', '')
        if target_player.lower() in h.lower() or target_player.lower() in a.lower():
            player_matches.append(m)
            
    print(f"Found {len(player_matches)} matches for {target_player}")
    
    leagues = [m.get('league', 'N/A') for m in player_matches]
    counts = Counter(leagues)
    
    print("-" * 50)
    print("League Distribution:")
    for league, count in counts.most_common():
        print(f"{league}: {count}")
    print("-" * 50)

except Exception as e:
    print(f"Error: {e}")
