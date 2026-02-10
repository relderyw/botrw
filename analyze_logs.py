
import json
import sys

log_file_path = r"w:\PYTHON\bot-esoccer\logs.1770684397030.json"

try:
    with open(log_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Loaded {len(data)} log entries.")
    
    keywords = ["Confidence", "Dica ignorada", "OPORTUNIDADE", "WARN", "FAITH", "Sheva", "Yerema", "Cold Streak"]
    

    with open(r"w:\PYTHON\bot-esoccer\analysis_result.txt", "w", encoding="utf-8") as out_f:
        out_f.write("-" * 50 + "\n")
        out_f.write(f"Searching for keywords: {keywords}\n")
        out_f.write("-" * 50 + "\n")

        found_count = 0
        for entry in data:
            message = entry.get('message', '')
            if any(k.lower() in message.lower() for k in keywords):
                out_f.write(f"[{entry.get('timestamp','?')}] {message}\n")
                found_count += 1
                
        out_f.write("-" * 50 + "\n")
        out_f.write(f"Found {found_count} matching entries.\n")
        print(f"Analysis saved to analysis_result.txt. Found {found_count} entries.")

except Exception as e:
    print(f"Error reading or parsing log file: {e}")
