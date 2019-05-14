from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

frame2 = spark.createDataFrame(
    [(1, "ONE", date(2018, 10, 22)), (2, "TWO", date(2019, 3, 12)), (3, "THREE", date(2018, 1, 1))],
    schema=StructType([
        StructField("id", IntegerType(), False),
        StructField("label", StringType(), True),
        StructField("mydate", DateType(), True)]))

frame = (
    spark.range(5)
        .withColumn("foo", col("id") % 2)
        .withColumn("bar", lit("2").cast("tinyint"))
        .withColumn("baz", when(col("id") * 5 + 10 > 22, col("bar") * col("foo")))
        .withColumn("date", date_add(to_date(lit("20181025"), "yyyyMMdd"), 1))
        .join(frame2, ["id"], how="left")
        .filter(col("id") >= 1)
        .filter(col("mydate") <= date(2018, 10, 17) )#"2018-10-17")
)

frame.show()
frame.explain(True)

# What does the query plan tell you about the order of the filter operations?
# → Spark's Catalyst moves them, because it “sees” it can do the order of
# operations more efficiently.

# What does it tell you about the datefilter? Do you see an easy performance
# boost there?
# → There's a cast to string. That's not very efficient because string
# comparisons are more computationally expensive than e.g. integer comparisons.
# Behind the scenes, dates are stored as a numeric type (e.g. the number of
# days since some reference). Indeed, change the date filter to e.g.
# `.filter(col("mydate") <= date(2018, 10, 17) )`
# and observe the filter changing to 17821, which is indeed the number of days
# since Jan 1st, 1970, which in IT is a common reference.
# How does changing the nullable flag of the column "mydate" impact the queryplan?
