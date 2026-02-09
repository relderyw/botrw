import requests
import json

base_url = "https://rwtips-r943.onrender.com/api/rw-matches"

def test_params(name, params):
    print(f"\nTesting params: {name} -> {params}")
    try:
        response = requests.get(base_url, params=params, timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            matches = data if isinstance(data, list) else data.get('partidas', [])
            print(f"Matches found: {len(matches)}")
            if matches:
                 # Check if the filtering actually worked (i.e. if returned matches match the params)
                 m = matches[0]
                 print(f"Sample match: Home={m.get('homeTeam')} vs Away={m.get('awayTeam')}")
        else:
            print("Request failed")
    except Exception as e:
        print(f"Error: {e}")

# Use known player names from verified output: DECIMATOR vs HAYMAKER or similar
p1 = "DECIMATOR"
p2 = "HAYMAKER"

test_params("homeTeam/awayTeam", {'homeTeam': p1, 'awayTeam': p2})
test_params("home_player/away_player", {'home_player': p1, 'away_player': p2})
test_params("jogador (p1 only)", {'jogador': p1, 'limit': 5}) # Baseline
