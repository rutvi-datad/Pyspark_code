from pyspark.sql.functions import count, desc, col, rank, asc , countDistinct 
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession  

spark = SparkSession.builder.appName("ProficientHacker").getOrCreate()

submissions_data = spark.read.csv("submissions.csv",header=True)
hackers_data = spark.read.format("json").load("hackers.json")

# Count number of submissions per hacker per day
daily_counts = submissions_data.groupBy("submission_date", "hacker_id") \
                               .agg(count("*").alias("total_submissions"))

# Partition by submission_date so ranking is done per day
windowSpec = Window.partitionBy("submission_date") \
                   .orderBy(desc("total_submissions"), asc("hacker_id"))

# Rank hackers within each day
max_hacker = daily_counts.withColumn("rnk", rank().over(windowSpec)) \
                         .filter(col("rnk") == 1) \
                         .select("submission_date", "hacker_id")

# Count distinct hackers who submitted each day
consistent_hackers = submissions_data.groupBy("submission_date") \
                                     .agg(countDistinct("hacker_id").alias("total_hackers"))

# Join results
result = max_hacker.join(consistent_hackers, "submission_date") \
                   .join(hackers_data, "hacker_id") \
                   .orderBy("submission_date")  \
                   .select("submission_date", "total_hackers", "hacker_id", "name")

result.show()

# result.write.parquet("D:/training/PySpark-git/output_parquet")