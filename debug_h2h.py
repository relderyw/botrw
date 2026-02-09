import requests

url = "https://sensorfifa.com.br/api/matches/h2h/Carlos/Kevin"
# Or a known existing pair if Carlos/Kevin is hypothetical.

try:
    print(f"Fetch URL: {url}")
    response = requests.get(url, timeout=15)
    print(f"Status Code: {response.status_code}")
    print(f"Content Type: {response.headers.get('Content-Type')}")
    print(f"Raw Text Preview (first 500 chars):")
    print(response.text[:500])
    try:
        data = response.json()
        print("JSON Parsed Successfully")
    except Exception as ie:
        print(f"JSON Parse Error: {ie}")

except Exception as e:
    print(f"Request Error: {e}")
