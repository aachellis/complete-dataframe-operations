#Importing necessery modules for pyspark SQL operations
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Creating a Spark Session for SQL and Dataframe Operations
spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Spark SQL Basic Functions").getOrCreate()

#Creating Data Frames by reading CSV files
cases = spark.read.csv("CovidKor\\Case.csv", inferSchema = True, header = True)
regions = spark.read.csv("CovidKor\\Region.csv", inferSchema = True, header = True)

#Creating UDF Function
def casesHighLow(confirmed):
    if confirmed < 50:
        return "low"
    else:
        return "high"

#Convert to a UDF Function
casesHighLowUDF = udf(casesHighLow, StringType())
#creating a new column to implement the UDF function
cases_with_udf = cases.withColumn("High_Low",casesHighLowUDF(cases["confirmed"]))
cases_with_udf.show()
