from pyspark.sql import SparkSession
from .utils import clean_column

def run_job(input_path):

    spark = SparkSession.builder.appName("MySparkJob").getOrCreate()
    df = spark.read.csv(input_path, header=True)
    df_clean = clean_column(df)
    df_clean.show()
    spark.stop()
