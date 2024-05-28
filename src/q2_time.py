from typing import List, Tuple
import json
import emoji

def count_each_emoji(text):
    emoji_counter = defaultdict(int)
    for char in text:
        if char in emoji.EMOJI_DATA:
            emoji_counter[char] += 1
    return emoji_counter

from collections import defaultdict
def q2_time(file_path: str) -> List[Tuple[str, int]]:
    with open(file_path, 'r') as f:
        data = [json.loads(line)['content']  for line in f.readlines()]
    single_string = " ".join(data)
    emoji_counts = count_each_emoji(single_string)
    lista=list(emoji_counts.items())
    sorted_emoji_counts = sorted(lista, key=lambda x: x[1], reverse=True)
    top_10 = sorted_emoji_counts[:10]
    print (top_10)
q2_time('src/farmers-protest-tweets-2021-2-4.json')
