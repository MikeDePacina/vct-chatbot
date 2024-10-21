from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.getOrCreate()

cwd = os.getcwd()

vct_intl_mapping_data = spark.read.json("./mappings/mapping_data_intl.json")

vct_intl_mapping_data.printSchema()