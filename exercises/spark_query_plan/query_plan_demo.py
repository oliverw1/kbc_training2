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
        .withColumn("bar", lit(2).cast("tinyint"))
        .withColumn("baz", when(col("id") * 5 + 10 > 22, col("bar") * col("foo")))
        .withColumn("date", date_add(to_date(lit("20181025"), "yyyyMMdd"), 1))
        .join(frame2, ["id"], how="left")
        .filter(col("id") >= 1)
        .filter(col("mydate") <= "2018-10-17")
)

frame.show()
frame.explain(True)

# What does the query plan tell you about the order of the filter operations?
# What does it tell you about the datefilter? Do you see an easy performance boost there?
# How does changing the nullable flag of the column "mydate" impact the queryplan?
