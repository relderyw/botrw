
import requests
from collections import defaultdict, Counter

url = "https://rwtips-r943.onrender.com/api/rw-matches"
print(f"Fetching from {url}...")
try:
    r = requests.get(url, timeout=60)
    data = r.json()
    matches = data if isinstance(data, list) else data.get('partidas', [])
    
    player_leagues = defaultdict(set)
    
    for m in matches:
        h = m.get('homeTeam', '') or m.get('homeClub', '')
        a = m.get('awayTeam', '') or m.get('awayClub', '')
        l = m.get('league', '')
        
        if h: player_leagues[h].add(l)
        if a: player_leagues[a].add(l)
        
    print("Players with mixed leagues:")
    count = 0
    for p, leagues in player_leagues.items():
        if len(leagues) > 1:
            # Check if leagues have different durations
            durations = set()
            for l in leagues:
                if '6m' in l or '6 min' in l: durations.add(6)
                elif '8m' in l or '8 min' in l: durations.add(8)
                elif '12m' in l or '12 min' in l or 'GT' in l: durations.add(12)
            
            if len(durations) > 1:
                print(f"{p}: {leagues} -> Durations: {durations}")
                count += 1
                if count >= 5: break

except Exception as e:
    print(f"Error: {e}")
