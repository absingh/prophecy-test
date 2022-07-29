from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Source_0 = Source_0(spark)
    df_Source_1 = Source_1(spark)
    df_Join_1 = Join_1(spark, df_Source_0, df_Source_1)
    df_Filter_2 = Filter_2(spark, df_Join_1)
    df_Source_2 = Source_2(spark)
    df_Join_2 = Join_2(spark, df_Filter_2, df_Source_2)
    Target_1(spark, df_Join_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "2548/pipelines/Test1Pipe")
    MetricsCollector.start(spark)
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
