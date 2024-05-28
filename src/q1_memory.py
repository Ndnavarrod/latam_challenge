import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window
from typing import List, Tuple
import datetime

def process_chunk(data_chunk, spark):
    # Define the schema for the JSON data
    columns = ['date', 'user']
    
    # Create DataFrame from chunk data
    df = spark.createDataFrame(data_chunk, columns)
    
    # Group by date and count occurrences
    date_counts = df.groupBy("date").count().orderBy(desc("count"))
    
    # Get top 10 dates
    top_10_dates = date_counts.limit(10)
    joined_df = df.join(top_10_dates, on="date")

    # Group by date and user, count occurrences, and use window function to get the most frequent user per date
    user_counts = joined_df.groupBy("date", "user").count()
    window_spec = Window.partitionBy("date").orderBy(desc("count"))
    ranked_users = user_counts.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
    
    return ranked_users.select("date", "user")

def read_in_chunks(file_path, chunk_size=100000):
    #The chunk size is started in 100.000 because this size not increase to much the processing time if we want to improve the memory this number need be smaller
    #Open the file for read 

    with open(file_path, 'r') as f:
        #Iniatilize an empty list to store the data 
        chunk = []
        #Iterate over each line in the file
        for i, line in enumerate(f):
            try:
                #Parse the json l ine 
                json_line = json.loads(line)
                #Extract the necesary information
                date = json_line['date'].split('T')[0]
                user = json_line['user']['username']
                #Append the result to Chunk 
                chunk.append((date, user))
                # Check if the chunk siize is a multiple of chunk size if is happend return the value in case not continue append information
                if (i + 1) % chunk_size == 0:
                    yield chunk
                    chunk = []
            except (json.JSONDecodeError, KeyError):
                continue
        if chunk:
            yield chunk

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    spark = SparkSession.builder.appName('sparkdf').getOrCreate()
    
    aggregated_results = []
    
    for chunk in read_in_chunks(file_path):
        ranked_users = process_chunk(chunk, spark)
        aggregated_results.extend([(row['date'], row['user']) for row in ranked_users.collect()])
    
    # Create a final DataFrame from aggregated results
    final_df = spark.createDataFrame(aggregated_results, ["date", "user"])
    
    # Perform final aggregation to get the most frequent user per date
    final_user_counts = final_df.groupBy("date", "user").count()
    window_spec = Window.partitionBy("date").orderBy(desc("count"))
    final_ranked_users = final_user_counts.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
    
    result = final_ranked_users.select("date", "user")
    
    result_list = [(row['date'], row['user']) for row in result.collect()]
    spark.stop()
    
    return result_list

