"""Demo-ing the Spark DataFrame API.
"""

from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"

# Python-esque:
frame = spark.read.csv(str(csv_file_path), sep=";", header=True)
# Scala-like:
frame = (spark # With parentheses like this, you can add comments in long chains
         .read # This would not have been possible with the backslash
         .options(header="true", sep=";")
         .csv(str(csv_file_path))
         )
print(frame.schema)
frame.show()
frame.printSchema()

frame.select("Age").withColumn("age_in_10_years", F.col("age") + 10).show()

new_frame = frame.filter(frame.Age.isNotNull())
print(new_frame.count())

frame2 = frame.orderBy(F.col("Age").asc()).cache()
frame2.show()
result = frame2.collect()
print(result)  # A list of Row objects
# Python is zero-based indexing: the zeroth element is the _first_ in an iterable
print(type(result[0]))
# Access row attributes dynamically
print(result[0].Name)
