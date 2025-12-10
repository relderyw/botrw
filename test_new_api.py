import requests
import json

def test_fetch(player_name):
    print(f"Testing for player: {player_name}")
    url = "https://rwtips-r943.onrender.com/api/v1/historico/partidas-assincrono"
    params = {'jogador': player_name, 'limit': 10, 'page': 1}
    
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data_raw = response.json()
        print("Raw keys:", data_raw.keys())
        
        matches = data_raw.get('partidas', [])
        print(f"Matches found: {len(matches)}")
        
        if matches:
            m = matches[0]
            print("First match sample raw keys:", m.keys())
            
            # Verify normalization logic
            normalized = {
                'id': m.get('id'),
                'home_player': m.get('home_player'),
                'away_player': m.get('away_player'),
                'home_score_ht': m.get('halftime_score_home'),
                'home_score_ft': m.get('score_home')
            }
            print("Normalized sample:", normalized)
            
            # Check if critical fields are present
            if 'halftime_score_home' in m:
                print("SUCCESS: 'halftime_score_home' field present.")
            else:
                print("FAILURE: 'halftime_score_home' field MISSING.")
        else:
            print("No matches to verify structure.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_fetch("Fred")
