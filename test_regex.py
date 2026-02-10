
import re

leagues = [
    "Battle 6m", 
    "Battle 8m", 
    "H2H 8m", 
    "GT League",
    "E-Soccer - Battle - 8 mins play",
    "Volta - 6 mins play",
    "Simulated Reality League - 12 mins play"
]

regex = r'(\d+)\s*(?:m|min|mins|minutos)'

print(f"Testing regex: {regex}")

for l in leagues:
    match = re.search(regex, l, re.IGNORECASE)
    if match:
        print(f"'{l}' -> {match.group(1)} mins")
    else:
        print(f"'{l}' -> NO MATCH")

print("-" * 20)
print("Testing previous regex:")
prev_regex = r'(\d+)\s*(?:min|mins|minutos)'
for l in leagues:
    match = re.search(prev_regex, l, re.IGNORECASE)
    if match:
        print(f"'{l}' -> {match.group(1)} mins")
    else:
        print(f"'{l}' -> NO MATCH")
