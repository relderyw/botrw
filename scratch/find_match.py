import requests
import json

HISTORY_URL = "https://rwtips-k8j2.onrender.com/api/history"
r = requests.get(HISTORY_URL, params={'page': 1, 'limit': 200})
if r.status_code == 200:
    results = r.json().get('results', [])
    for m in results:
        h = str(m.get('home_nick', '')).upper()
        a = str(m.get('away_nick', '')).upper()
        if ('ALUKARD' in h and 'YETI' in a) or ('YETI' in h and 'ALUKARD' in a):
             print(f"FOUND: {h} vs {a} | HT: {m.get('home_score_ht')}-{m.get('away_score_ht')} | FT: {m.get('home_score_ft')}-{m.get('away_score_ft')}")
             print(json.dumps(m, indent=2))
else:
    print(f"Error: {r.status_code}")
