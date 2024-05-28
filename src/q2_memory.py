import json
from collections import defaultdict
import emoji
from typing import List, Tuple

def count_each_emoji_chunk(chunk):
    # Counter for emojis
    emoji_counter = defaultdict(int)
    
    # Iterate over each text chunk
    for text in chunk:
        # Explore each character in the text and check if it's an emoji
        for char in text:
            if char in emoji.EMOJI_DATA:
                emoji_counter[char] += 1
    
    return emoji_counter

def count_each_emoji(text):
    # Chunk size
    chunk_size = 1000
    
    # Split the text into chunks
    chunked_text = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
    
    # Initialize the emoji counter
    emoji_counter = defaultdict(int)
    
    # Process each chunk
    for chunk in chunked_text:
        # Count emojis in the current chunk
        chunk_counts = count_each_emoji_chunk(chunk)
        
        # Combine emoji counts from each chunk
        for emoji_char, count in chunk_counts.items():
            emoji_counter[emoji_char] += count
    
    return emoji_counter

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # Read the file and extract the content
    with open(file_path, 'r') as f:
        data = [json.loads(line)['content']  for line in f.readlines()]
    
    # Combine all the content into a single string
    single_string = " ".join(data)
    
    # Count the occurrences of each emoji
    emoji_counts = count_each_emoji(single_string)
    
    # Convert the counter to a list of tuples and sort by count in descending order
    sorted_emoji_counts = sorted(emoji_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Return the top 10 emojis
    return sorted_emoji_counts[:10]
