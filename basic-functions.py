#Importing necessery modules for pyspark SQL operations
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Creating a Spark Session for SQL and Dataframe Operations
spark = SparkSession.builder.master("local").config("spark.sql.wirehouse.dir","c:\\tmp").appName("Spark SQL Basic Functions").getOrCreate()

#Creating Data Frames by reading CSV files
cases = spark.read.csv("CovidKor\\Case.csv", inferSchema = True, header = True)
regions = spark.read.csv("CovidKor\\Region.csv", inferSchema = True, header = True)
#Seeing the rows of the filee
cases.show()
#The .toPandas() function converts a spark dataframe into a pandas Dataframe which is easier to show
cases.limit(10).toPandas() #Not showing any results. I don't know why.
#Change Column Names
cases = cases.withColumnRenamed("infection_case","infection_source")
cases.limit(3).show()
#Select Columns
cases.select(" case_id","province","city","infection_source").limit(3).show()
#Sorting Functions
cases.sort("confirmed").show()
#Sorting as descending
cases.sort(col("confirmed").desc()).show()
#Type Casting in DataFrame
cases = cases.withColumn("confirmed",col("confirmed").cast(IntegerType()))
#cases.show(10)
#Filter Function
cases.filter((cases["province"] == "Seoul") & (cases["confirmed"] > 100)).show()
#GroupBy Function
cases.groupBy(["province","city"]).agg( \
        sum("confirmed").alias("Total Confirmed"), \
        avg("confirmed").alias("Avarage Confirmed"), \
        max("confirmed").alias("Maximum Confirmed in the City")).show()
#Joining Operation
cases_joined = cases.join(regions, on = ["city","province"], how = "inner")
cases_joined.show()
#BroadCast/Map Side Joins
'''
In spark operation where you might want to apply multiple operations to a particular key. But assuming that the data for each key in the Big table is large, it will involve a lot of data movement. And sometimes so much that the application itself breaks. A small optimization then you can do when joining on such big tables(assuming the other table is small) is to broadcast the small table to each machine/node when you perform a join.You can do this easily using the broadcast keyword.
'''
cases_joined = cases.join(broadcast(regions), on = ["city","province"], how = "left").show()
#Using SQL in DataFrame
cases.registerTempTable("cases_table")
newDF = spark.sql("select * from cases_table where confirmed > 100")
newDF.show()
