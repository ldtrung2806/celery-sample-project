import findspark
findspark.init()
from pyspark.sql import SparkSession
from dependencies import logging


def start_spark(log_name: str):
    spark_sess = SparkSession.builder \
    .appName("App") \
    .master("local[*]")\
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()
    spark_sess.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark_sess.conf.set("parquet.enable.summary-metadata", "false")
    return spark_sess, logging.Logger(spark_sess, log_name)