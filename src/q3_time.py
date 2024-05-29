
from pyspark.sql import SparkSession
from typing import List, Tuple
from pyspark.sql.functions import col, count,desc
from pyspark.sql.functions import explode

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    #Create the spark session

    spark = SparkSession.builder.appName('sparkdf').getOrCreate() 
    df = spark.read.json(file_path)

    #Only keep the  column that need that is user list mentioned and drop the none values
    df = df.select( col("mentionedUsers.username").alias("user")).dropna()

    #Expand the list of user to have one row per each mention in a twitt
    df_expanded = df.select(explode(df.user).alias("user"))

    #Count The repetion of the name using group by 
    df_counts=df_expanded.groupBy('user').count()

    #Limit only the top 10 
    top_10= df_counts.orderBy(col("count").desc()).limit(10)

    #Put in the requeired output expected values 
    top_10_list: List[Tuple[str, int]] = [(row["user"], row["count"]) for row in top_10.collect()]
  
    return top_10_list


