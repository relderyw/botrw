
import requests
import re

url = "https://rwtips-r943.onrender.com/api/rw-matches"
print(f"Fetching from {url}...")
try:
    r = requests.get(url, timeout=60)
    data = r.json()
    matches = data if isinstance(data, list) else data.get('partidas', [])
    print(f"Fetched {len(matches)} matches.")
    
    target_player = "Lapinzz10L"
    
    # 1. Filter by Player
    player_matches = []
    for m in matches:
        h = m.get('homeTeam', '') or m.get('homeClub', '')
        a = m.get('awayTeam', '') or m.get('awayClub', '')
        if target_player.lower() in h.lower() or target_player.lower() in a.lower():
            player_matches.append(m)
            
    print(f"Found {len(player_matches)} matches for {target_player}")
    
    # 2. Filter by Duration (Simulating logic)
    target_league_6m = "Volta - 6 mins"
    target_duration = 6
    
    filtered_6m = []
    for m in player_matches:
        m_league = m.get('league', '')
        m_duration_match = re.search(r'(\d+)\s*(?:m|min|mins|minutos)', m_league, re.IGNORECASE)
        m_duration = None
        if m_duration_match:
            m_duration = int(m_duration_match.group(1))
        elif 'GT League' in m_league or 'GT Leagues' in m_league:
            m_duration = 12
            
        if m_duration is not None:
            if m_duration != target_duration:
                continue
        else:
            continue
        filtered_6m.append(m)
        
    print(f"Filtered to 6 mins: {len(filtered_6m)} matches")
    leagues_6m = set(m.get('league') for m in filtered_6m)
    print(f"Distinct Leagues (6m): {leagues_6m}")
    
    # 3. Filter by Duration 12m (GT)
    target_duration = 12
    filtered_12m = []
    for m in player_matches:
        m_league = m.get('league', '')
        m_duration_match = re.search(r'(\d+)\s*(?:m|min|mins|minutos)', m_league, re.IGNORECASE)
        m_duration = None
        if m_duration_match:
            m_duration = int(m_duration_match.group(1))
        elif 'GT League' in m_league or 'GT Leagues' in m_league:
            m_duration = 12
            
        if m_duration is not None:
            if m_duration != target_duration:
                continue
        else:
            continue
        filtered_12m.append(m)

    print(f"Filtered to 12 mins: {len(filtered_12m)} matches")
    leagues_12m = set(m.get('league') for m in filtered_12m)
    print(f"Distinct Leagues (12m): {leagues_12m}")

except Exception as e:
    print(f"Error: {e}")
