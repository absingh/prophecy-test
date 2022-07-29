from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_IN1 = IN1(spark)
    df_IN2 = IN2(spark)
    df_Join_1 = Join_1(spark, df_IN1, df_IN2)
    df_Filter_2 = Filter_2(spark, df_Join_1)
    df_IN3 = IN3(spark)
    df_Join_2 = Join_2(spark, df_Filter_2, df_IN3)
    Target_1(spark, df_Join_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "2548/pipelines/Test1Pipe")
    MetricsCollector.start(spark = spark, pipelineId = "2548/pipelines/Test1Pipe")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
