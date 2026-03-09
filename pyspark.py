from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()
submissions = spark.read.option("header", True).option("inferSchema", True) \
.csv("/Volumes/workspace/default/pyspark_volume/Submissions.csv")



hackers = spark.read.option("header", True).option("inferSchema", True) \
.csv("/Volumes/workspace/default/pyspark_volume/Hackers.csv")
submissions.show()
hackers.show()
submissions = submissions.withColumn(
    "submission_date",
    to_date(col("submission_date"), "M/d/yyyy")
)
submissions.write.mode("overwrite").json("/Volumes/workspace/default/pyspark_volume/submissions_json")

submissions.write.mode("overwrite").csv(
"/Volumes/workspace/default/pyspark_volume/submissions_csv", header=True)

hackers.write.mode("overwrite").json("/Volumes/workspace/default/pyspark_volume/hackers_json")

hackers.write.mode("overwrite").csv(
"/Volumes/workspace/default/pyspark_volume/hackers_csv", header=True)
daily_submissions = submissions.groupBy(
    "submission_date","hacker_id"
).agg(
    count("*").alias("total_submissions")
)
windowSpec = Window.partitionBy("submission_date") \
.orderBy(desc("total_submissions"), asc("hacker_id"))

max_hacker = daily_submissions.withColumn(
    "rn",
    row_number().over(windowSpec)
)
top_hacker = max_hacker.filter(col("rn")==1)
first_date = submissions.agg(min("submission_date")).collect()[0][0]
hacker_days = submissions.select("hacker_id","submission_date").distinct()
windowSpec = Window.partitionBy("hacker_id").orderBy("submission_date")
hacker_days = hacker_days.withColumn(
    "day_number",
    row_number().over(windowSpec)
)
hacker_days = hacker_days.withColumn(
    "actual_day",
    datediff(col("submission_date"), lit(first_date)) + 1
)
continuous = hacker_days.filter(
    col("day_number")==col("actual_day")
)
daily_continuous = continuous.groupBy("submission_date") \
.agg(countDistinct("hacker_id").alias("total_hackers"))
result = daily_continuous.join(
    top_hacker,"submission_date"
).join(
    hackers,"hacker_id"
).select(
    "submission_date",
    "total_hackers",
    "hacker_id",
    "name"
).orderBy("submission_date")

result.show()
result.write.mode("overwrite") \
.parquet("/Volumes/workspace/default/pyspark_volume/result_parquet")
display(dbutils.fs.ls("/Volumes/workspace/default/pyspark_volume/"))
result.show()

