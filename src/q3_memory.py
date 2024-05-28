from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Create the Spark session
    spark = SparkSession.builder.appName('sparkdf').getOrCreate() 
    
    # Read JSON file and select relevant column, dropping rows with null values
    df = spark.read.json(file_path).select(col("mentionedUsers.username").alias("user")).dropna()
    
    # Expand the list of users and count occurrences in one step using groupBy and explode
    df_counts = df.select(explode(col("user")).alias("user")).groupBy('user').count()
    
    # Get top 10 users by count
    top_10 = df_counts.orderBy(col("count").desc()).limit(10)
    
    # Collect results and convert to the required output format
    top_10_list = [(row["user"], row["count"]) for row in top_10.collect()]
    
    # Stop the Spark session
    spark.stop()
    
    return top_10_list
