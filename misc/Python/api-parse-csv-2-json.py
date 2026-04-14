import requests
import json

response = requests.get("https://coderbyte.com/api/challenges/logs/user-info-csv")

# write your solution here
if response.status_code == 200:

  csv_text = response.text.strip()

  lines = csv_text.split('\n')

  data_lines = lines[1:] if len(lines) > 0 else []
  
  records = []
  for line in data_lines:
    if line.strip():
      parts = line.strip().split(',')
      if len(parts) >= 3:
        record = {
          "name": parts[0].strip(),
          "email": parts[1].strip(),
          "phone": parts[2].strip(),
        }
        records.append(record)

  records.sort(key=lambda x : x["email"])
  
  json_output = json.dumps(records, indent = None)
  print(json_output)

else:
  print(f"Error: {response.status_code}")