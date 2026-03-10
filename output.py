from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("ContestAnalysis").getOrCreate()


hackers = spark.read.option("multiline", True)\
    .json("D:/pyspark/data/hackers.json")


submissions = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("D:/pyspark/data/submit.csv")


submissions = submissions.withColumn(
    "submission_date",
    to_date("submission_date")
)


contest_start = submissions.select(
    min("submission_date").alias("start_date")
)

start_date = contest_start.collect()[0]["start_date"]

daily_count = submissions.groupBy(
    "submission_date", "hacker_id"
).agg(
    count("*").alias("total_submissions")
)


running_days1 = submissions.select(
    "submission_date",
    "hacker_id"
).distinct()

running_days = running_days1.withColumn(
    "days_submitted",
    size(
        collect_set("submission_date").over(
            Window.partitionBy("hacker_id")
            .orderBy("submission_date")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
)

consistent = running_days.withColumn(
    "expected_days",
    datediff(col("submission_date"), lit(start_date)) + 1
).filter(
    col("days_submitted") == col("expected_days")
).groupBy(
    "submission_date"
).agg(
    countDistinct("hacker_id").alias("unique_hackers")
)


window_spec = Window.partitionBy("submission_date")\
    .orderBy(col("total_submissions").desc(), col("hacker_id").asc())

max_per_day = daily_count.withColumn(
    "rn",
    row_number().over(window_spec)
).filter(
    col("rn") == 1
)


final_df = max_per_day.join(
    consistent,
    "submission_date"
).join(
    hackers,
    "hacker_id"
).select(
    "submission_date",
    "unique_hackers",
    "hacker_id",
    "name"
).orderBy("submission_date")

final_df.coalesce(1).write.mode("overwrite")\
    .parquet("/Workspace/Users/enigofleming.xavier@datad.co/pyspark/data/contest_result.parquet")


final_df.display()
