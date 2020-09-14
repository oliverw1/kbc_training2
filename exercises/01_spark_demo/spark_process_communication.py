from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

spark = SparkSession.builder.getOrCreate()


# dataframe = spark.range(5)
# dataframe.show()
#
#
# frame = spark.createDataFrame(
#     data=[("hello", "world", "!"), (None, "and", "bye!"), ("One", "Two", "Three")],
#     schema=("a", "b", "c"),
# )
#
#
# print(frame.rdd.map(lambda row: row[1].upper()).collect())
#
#
# def titular_mapping(row):
#     print("this is all executed on the workers, this is not the driver context")
#     return row[2].title(), row[1].title()
#
#
# print(frame.rdd.map(titular_mapping).collect())
#
#
# frame.select(upper(frame["b"])).show()
