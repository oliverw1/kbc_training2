"""Demo-ing the Spark DataFrame API.
"""

from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"

# Python-esque:
frame = spark.read.csv(str(csv_file_path), sep=";", header=True)
frame.show()
frame.printSchema()

print(frame.schema)
print(frame.columns)
frame.describe().show(truncate=False)
