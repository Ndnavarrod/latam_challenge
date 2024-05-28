from typing import List, Tuple
from datetime import datetime
import json
from collections import defaultdict
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count,desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from memory_profiler import profile

@profile
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
        #El proceso se hace con spark para trabajar la informacion de manera columnar. 
        spark = SparkSession.builder.appName('sparkdf').getOrCreate() 
        df = spark.read.json(file_path)
        df = df.withColumn("date", col("date").substr(1, 10))
        df = df.select("date", col("user.username").alias("user")).dropna()
        

        date_counts = df.groupBy("date").count().orderBy(desc("count"))


        top_10_dates = date_counts.limit(10)
        joined_df = df.join(top_10_dates, on="date")

# Group by date and user, count occurrences, and use window function to get the most frequent user per date
        user_counts = joined_df.groupBy("date", "user").count()
        window_spec = Window.partitionBy("date").orderBy(desc("count"))
        ranked_users = user_counts.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
        result = ranked_users.select("date", "user")
        
        result_list:List[Tuple[str, int]] = [(row['date'], row['user'])for row in result.collect()]
        spark.stop()
        return(result_list)
