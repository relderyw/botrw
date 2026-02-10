
import requests
import json

url = "https://rwtips-r943.onrender.com/api/rw-matches"
try:
    r = requests.get(url, timeout=30)
    data = r.json()
    matches = data if isinstance(data, list) else data.get('partidas', [])
    
    unique_leagues = sorted(list(set(m.get('league', 'N/A') for m in matches)))
    
    print("Unique Leagues in History API:")
    for l in unique_leagues:
        print(f"'{l}'")
        
except Exception as e:
    print(e)
