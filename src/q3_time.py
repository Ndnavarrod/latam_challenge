
from pyspark.sql import SparkSession
from typing import List, Tuple
from pyspark.sql.functions import col, count,desc
from pyspark.sql.functions import explode

def q3_time(file_path: str) -> List[Tuple[str, int]]:

