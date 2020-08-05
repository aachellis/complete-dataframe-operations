from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Windown Function in Spark").getOrCreate()
timeprovince = spark.read.csv("CovidKor\\TimeProvince.csv", inferSchema = True, header = True)
cases = spark.read.csv("CovidKor\\Case.csv",inferSchema = True,header = True)
timeprovince.show(3)
windowSpec = Window().partitionBy("province").orderBy(cases["confirmed"].desc())
cases.withColumn("Rank",rank().over(windowSpec)).show()

#Lag Variable
windowSpec = Window().partitionBy("province").orderBy("date")
timeprovincewithlag = timeprovince.withColumn("lag_7",lag("confirmed",7).over(windowSpec))
timeprovincewithlag.filter(timeprovincewithlag.date > "2020-03-10").show()

#Rolling Aggregations
windoSpec = Window().partitionBy("province").orderBy("date").rowsBetween(-6,0)
timeProvinceWithRoll = timeprovince.withColumn("roll_7_confirmed",mean("confirmed").over(windowSpec))
timeProvinceWithRoll.filter(timeProvinceWithRoll.date > "2020-03-10").show()
