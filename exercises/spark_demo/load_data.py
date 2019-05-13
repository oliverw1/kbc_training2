"""Demo-ing the Spark DataFrame API.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"

# Python-esque:
frame = spark.read.csv(str(csv_file_path), sep=";", header=True)
# Scala-like:
# frame = spark.read.options(header="true", sep=";").csv(str(csv_file_path))
frame.show()
frame.printSchema()


frame.select("Age").withColumn("age_in_10_years", col("age") + 10).show()


print(frame.filter(frame.Age.isNotNull()).count())


frame2 = frame.orderBy(col("Age").asc()).cache()
frame2.show()
result = frame2.collect()
print(result)
print(type(result[0]))
print(result[0].Name)
