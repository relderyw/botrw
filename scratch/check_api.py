import requests
import json

HISTORY_URL = "https://rwtips-k8j2.onrender.com/api/history"
r = requests.get(HISTORY_URL, params={'page': 1, 'limit': 5})
if r.status_code == 200:
    print(json.dumps(r.json(), indent=2))
else:
    print(f"Error: {r.status_code}")
