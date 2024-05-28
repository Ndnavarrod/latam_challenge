from typing import List, Tuple
import json
import emoji
from collections import defaultdict

# Identify each character that is a emoji and save the count in  default dict
def count_each_emoji(text):
    #input Text: is a string 
    #output: the counter of each emoji
    emoji_counter = defaultdict(int)
    for char in text:
        #Explore each of the char and identify if is a emoji
        if char in emoji.EMOJI_DATA:
            emoji_counter[char] += 1
    return emoji_counter



def q2_time(file_path: str) -> List[Tuple[str, int]]:
    #Open the input file and extract the content of each twit
    with open(file_path, 'r') as f:
        data = [json.loads(line)['content']  for line in f.readlines()]
    #Join all the data as a single string
    single_string = " ".join(data)
    #Count all of the emojis in this text
    emoji_counts = count_each_emoji(single_string)
    lista=list(emoji_counts.items())
    # Sorted the emojis having first the emojis with more repetition
    sorted_emoji_counts = sorted(lista, key=lambda x: x[1], reverse=True)
    #Take the first 10
    top_10 = sorted_emoji_counts[:10]
    return top_10
