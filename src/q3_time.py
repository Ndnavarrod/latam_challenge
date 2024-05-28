from typing import List, Tuple
import json
def extract_usernames(data):
    usernames = []
    for entry in data:
        if isinstance(entry, list):
            for user in entry:
                if isinstance(user, dict) and 'username' in user:
                    usernames.append(user['username'])
                else:
                    print(f"Warning: 'username' key not found in user entry: {user}")
        elif entry is None:
            continue
       
    return usernames

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    # En este caso como la mencion no existe en todos los twitss se agrega una validacion de que si exista.
    data = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                json_line = json.loads(line)
                if 'mentionedUsers' in json_line:
                    data.append(json_line['mentionedUsers'])
                else:
                    continue
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON format in line: {line.strip()}")
    usernames = extract_usernames(data)
    print(usernames)

 
q3_time('src/farmers-protest-tweets-2021-2-4.json')