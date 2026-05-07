import requests
import json

HISTORY_URL = "https://rwtips-k8j2.onrender.com/api/history"
r = requests.get(HISTORY_URL, params={'page': 1, 'limit': 10})
if r.status_code == 200:
    results = r.json().get('results', [])
    for m in results:
        if m.get('home_nick') == 'Alukard' or m.get('away_nick') == 'Yeti':
             print(json.dumps(m, indent=2))
else:
    print(f"Error: {r.status_code}")
