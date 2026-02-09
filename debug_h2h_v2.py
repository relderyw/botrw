import requests

# Try the specific H2H endpoint from the failing code
url = "https://sensorfifa.com.br/api/matches/h2h/Carlos/Kevin" 

print(f"Testing URL: {url}")
try:
    response = requests.get(url, timeout=10)
    print(f"Status Code: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    
    if response.status_code == 200:
        try:
            data = response.json()
            print("Response IS valid JSON.")
            print(data)
        except Exception as e:
            print("Response is NOT valid JSON.")
            print(f"Error: {e}")
            print("Response Text Preview:")
            print(response.text[:200])  # Show first 200 chars
    else:
        print("Request failed with status code != 200")
        print(response.text[:200])

except Exception as e:
    print(f"Exception during request: {e}")

# Also try searching using the main matches endpoint if the H2H one fails?
# But let's first confirm the H2H endpoint status.
