import requests
import json

LIVE_API_URL = "https://app3.caveiratips.com.br/api/live-events/"
PLAYER_API_URL = "https://rwtips-r943.onrender.com/api/v1/historico/partidas-assincrono"
H2H_API_URL = "https://rwtips-r943.onrender.com/api/v1/historico/confronto/{home}/{away}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 OPR/124.0.0.0",
    "Accept": "*/*",
    "Referer": "https://app3.caveiratips.com.br/jogos-ao-vivo",
    "Origin": "https://app3.caveiratips.com.br",
    "Sec-Ch-Ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Opera Air";v="124"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=1, i"
}

def test_live_api():
    print("Testing Live API...")
    try:
        response = requests.get(LIVE_API_URL, headers=HEADERS, timeout=10)
        response.raise_for_status()
        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Live API returned non-JSON response.")
            print(response.text[:500])
            return None

        print(f"Live API Status: {response.status_code}")
        if isinstance(data, list):
            print(f"Live API returned a list of {len(data)} items.")
            if len(data) > 0:
                print("Sample Match Data:")
                print(json.dumps(data[0], indent=2))
                return data[0]
        elif isinstance(data, dict):
             print("Live API returned a dict.")
             print(json.dumps(data, indent=2))
             return data
        else:
            print("Live API returned unknown format.")
            return None
    except Exception as e:
        print(f"Live API Error: {e}")
        return None

def test_player_api(player_name):
    print(f"\nTesting Player API for {player_name}...")
    try:
        params = {"jogador": player_name, "limit": 5, "page": 1}
        response = requests.get(PLAYER_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"Player API Status: {response.status_code}")
        # print(json.dumps(data, indent=2)) # Too verbose
        if 'partidas' in data:
            print(f"Found {len(data['partidas'])} matches for {player_name}")
            if len(data['partidas']) > 0:
                print("Sample Match:")
                print(json.dumps(data['partidas'][0], indent=2))
        else:
             print("Unexpected Player API response structure")
             print(json.dumps(data, indent=2))

    except Exception as e:
        print(f"Player API Error: {e}")

def test_h2h_api(home, away):
    print(f"\nTesting H2H API for {home} vs {away}...")
    try:
        url = H2H_API_URL.format(home=home, away=away)
        params = {"limit": 5, "page": 1}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"H2H API Status: {response.status_code}")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"H2H API Error: {e}")

if __name__ == "__main__":
    test_live_api()
    test_player_api("Fred")
    test_h2h_api("Snail", "Tifosi")
