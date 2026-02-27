import requests
import datetime
import json
import traceback

def main():
    try:
        url = "https://rwtips-k8j2.onrender.com/api/history?page=1&limit=5"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        results = data.get('results', [])
        
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        print(f"Current UTC time: {now_utc.isoformat()}")
        
        for r in results:
            print(f"Match ID: {r.get('event_id')}")
            print(f"  Finished at: {r.get('finished_at')}")
            print(f"  Match Date: {r.get('match_date')}")
            print(f"  Match Time: {r.get('match_time')}")
            
    except Exception as e:
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
