"""Demo-ing the Spark DataFrame API.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

# The verbose way
fields = [StructField("name", StringType(), nullable=False),
          StructField("age", IntegerType(), nullable=True)]
users = spark.createDataFrame(
    [("Wim", 1), ("Conrad", 2)],
    schema=StructType(fields)
)

# A shorter way, with implicit assumptions
currencies = spark.createDataFrame(
    [("Euro", 1.0, 1), ("USD", 1.2, 1)],
    ("currency", "value", "random")
)

for frame in (users, currencies):
    frame.show()  # an action
    frame.printSchema()  # Not an action
