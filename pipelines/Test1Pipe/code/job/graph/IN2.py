from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def IN2(spark: SparkSession) -> DataFrame:
    return spark.read.option("header", True).option("sep", ",").csv("/in2.csv")
