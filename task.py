# !pip install pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, count, dense_rank, lit,sum
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("task").getOrCreate()

hackers = spark.read.option("multiline", True).json("hackers.json")
submissions = spark.read.option("header",True).option("inferschema",True).option("delimiter",",").csv("submissions.csv")

# Count of submission of each hacker on each day
daily_counts=submissions.groupBy("submission_date","hacker_id").count().withColumnRenamed("count","total_sub")
# daily_counts.orderBy("submission_date").show()

# giving the rank for each day (max submission or based on the hacker_id)
window1=Window.partitionBy("submission_date").orderBy(col("total_sub").desc(),col("hacker_id").asc())
max_sub = daily_counts.withColumn("r",row_number().over(window1))
# max_sub.show()

# getting distinct submission date and hacker id
base=submissions.select("submission_date","hacker_id").distinct()
# base.orderBy("submission_date").show()

# created window specifications to perform window functions
window2=Window.partitionBy("hacker_id").orderBy(col("submission_date"))
window3=Window.orderBy(col("submission_date"))

# counting the number of running days and the number of days hacker attended
with_counts=base.withColumn("hacker_running_days",count(lit(1)).over(window2)).withColumn("total_running_days",dense_rank().over(window3))
# with_counts.show()

# based on the running days and the hacker's attendance, separating the count of hackers attended continuosly till the date
consisten=with_counts.groupBy("submission_date").agg(sum((col("hacker_running_days") == col("total_running_days")).cast("int")).alias("counting"))
# consisten.show()

# joining the consistent hackers and count of max submissions per day
result=max_sub.join(hackers,on="hacker_id",how="inner").join(consisten,on="submission_date",how="inner").filter(col("r")==1).orderBy("submission_date")
result=result.select("submission_date","counting","hacker_id","name")
result.show()