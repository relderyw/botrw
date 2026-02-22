import requests

INTERNAL_API_URL = "https://rwtips-r943.onrender.com/api/app3/history"

def verify_internal():
    r = requests.get(INTERNAL_API_URL, params={"page": 1, "limit": 300})
    if r.status_code != 200:
        print(f"Error: {r.status_code}")
        return

    results = r.json().get('results', [])
    targets = [('SHEERPY', 'HOLIS')]

    print("--- SEARCH RESULTS ---")
    for m in results:
        home = m.get('home_player', '').upper()
        away = m.get('away_player', '').upper()
        for t_home, t_away in targets:
            if t_home in home and t_away in away:
                print(f"KEYS: {m.keys()}")
                print(f"homeTeamName: {m.get('homeTeamName')}, awayTeamName: {m.get('awayTeamName')}")
                print(f"home_team: {m.get('home_team')}, away_team: {m.get('away_team')}")
                print("-" * 20)

if __name__ == "__main__":
    verify_internal()
