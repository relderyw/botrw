
import requests
import re
import time

# Mocking global cache/functions
player_stats_cache = {}
CACHE_TTL = 300
global_matches = []

def get_all_matches():
    global global_matches
    if global_matches: return global_matches
    print("Fetching matches...")
    try:
        r = requests.get("https://rwtips-r943.onrender.com/api/rw-matches", timeout=60)
        data = r.json()
        global_matches = data if isinstance(data, list) else data.get('partidas', [])
        print(f"Fetched {len(global_matches)} matches from API.")
        if len(global_matches) > 0:
            print(f"Sample match: {global_matches[0]}")
    except Exception as e:
        print(f"Error fetching matches: {e}")
        global_matches = []
    return global_matches


def fetch_player_individual_stats(player_name, target_league=None, use_cache=True):
    # Determinar duração alvo
    target_duration = None
    if target_league:
        match = re.search(r'(\d+)\s*(?:m|min|mins|minutos)', target_league, re.IGNORECASE)
        if match:
            target_duration = int(match.group(1))
        elif 'GT League' in target_league or 'GT Leagues' in target_league:
            target_duration = 12
            
    print(f"Target Duration for '{target_league}': {target_duration}")

    all_matches = get_all_matches()
    
    player_matches = []
    p_upper = player_name.upper()
    
    print(f"Searching for player: '{player_name}' in {len(all_matches)} matches")
    
    count_checked = 0
    for m in all_matches:
        count_checked += 1
        
        # Raw API keys
        h_norm = m.get('homeTeam', '') or m.get('homeClub', '')
        a_norm = m.get('awayTeam', '') or m.get('awayClub', '')
        
        if count_checked <= 3:
            print(f"DEBUG Match #{count_checked}: Home='{h_norm}', Away='{a_norm}', League='{m.get('league')}'")
        
        if player_name.lower() in h_norm.lower() or player_name.lower() in a_norm.lower():
            # DEBUG: Match found for player
            # print(f"Player match: {m.get('league')} - {h_norm} vs {a_norm}")
            
            # Filtro de Duração
            if target_duration:
                m_league = m.get('league', '')
                
                m_duration_match = re.search(r'(\d+)\s*(?:m|min|mins|minutos)', m_league, re.IGNORECASE)
                m_duration = None
                
                if m_duration_match:
                    m_duration = int(m_duration_match.group(1))
                elif 'GT League' in m_league or 'GT Leagues' in m_league:
                    m_duration = 12
                
                if m_duration is not None:
                    if m_duration != target_duration:
                        # print(f"Ignored {m_league} (Duration {m_duration} != {target_duration})")
                        continue
                else:
                    # print(f"Ignored {m_league} (No duration)")
                    continue 
            
            player_matches.append(m)
            
    print(f"Total entries found for player before limit: {len(player_matches)}")
    recent_matches = player_matches[:20]
    return recent_matches


# Test Case 1: 6 mins
print("\n--- TEST: fantazer in 6 mins league ---")
matches_6m = fetch_player_individual_stats("fantazer", target_league="Volta - 6 mins")
print(f"Returned {len(matches_6m)} matches.")
leagues_6m = set(m.get('league') for m in matches_6m)
print(f"Distinct Leagues: {leagues_6m}")

# Test Case 2: 8 mins
print("\n--- TEST: fantazer in 8 mins league ---")
matches_8m = fetch_player_individual_stats("fantazer", target_league="Battle 8m")
print(f"Returned {len(matches_8m)} matches.")
leagues_8m = set(m.get('league') for m in matches_8m)
print(f"Distinct Leagues: {leagues_8m}")

# Test Case 3: 12 mins (GT)
print("\n--- TEST: fantazer in 12 mins league ---")
matches_12m = fetch_player_individual_stats("fantazer", target_league="GT Leagues - 12 mins play")
print(f"Returned {len(matches_12m)} matches.")
leagues_12m = set(m.get('league') for m in matches_12m)
print(f"Distinct Leagues: {leagues_12m}")
