from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Windown Function in Spark").getOrCreate()
timeprovince = spark.read.csv("CovidKor\\TimeProvince.csv", inferSchema = True, header = True)
'''
Sometimes we may need to have the dataframe in flat format. This happens frequently in movie data where we may want to show genres as columns instead of rows. We can use pivot to do this.
'''
pivotedTimeProvince = timeprovince.groupBy("date").pivot("province").agg(sum("Confirmed").alias("confirmed"),sum("released").alias("released"))
pivotedTimeProvince.limit(10).show()
