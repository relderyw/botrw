import requests
from datetime import datetime, timedelta

HISTORY_API_URL = "https://rwtips-r943.onrender.com/api/app3/history"
GREEN365_API_URL = "https://api-v2.green365.com.br/api/v2/sport-events"
AUTH_HEADER = "Bearer 444c7677f71663b246a40600ff53a8880240086750fda243735e849cdeba9702"

def search_internal():
    print("Searching Internal API...")
    for page in range(1, 15):
        try:
            r = requests.get(HISTORY_API_URL, params={'page': page, 'limit': 50}, timeout=10)
            if r.status_code == 200:
                results = r.json().get('results', [])
                for m in results:
                    players = str(m).upper()
                    if 'AMBER' in players and 'LOTTA' in players:
                        print(f"FOUND in Internal (Page {page}): {m}")
                        return
        except Exception as e:
            print(f"Error internal: {e}")

def search_green365():
    print("Searching Green365 API...")
    headers = {"Authorization": AUTH_HEADER}
    for page in range(1, 10):
        try:
            params = {"page": page, "limit": 50, "sport": "esoccer", "status": "ended"}
            r = requests.get(GREEN365_API_URL, params=params, headers=headers, timeout=12)
            if r.status_code == 200:
                items = r.json().get('items', [])
                for item in items:
                    players = f"{item.get('home',{}).get('name','')} vs {item.get('away',{}).get('name','')}".upper()
                    if 'AMBER' in players and 'LOTTA' in players:
                        print(f"FOUND in Green365 (Page {page}): ID={item['id']}, HT={item.get('scoreHT')}, FT={item.get('score')}, Start={item.get('startTime')}")
                        return
        except Exception as e:
            print(f"Error green365: {e}")

search_internal()
search_green365()
