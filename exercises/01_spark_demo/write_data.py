"""Demo-ing the Spark DataFrame API.
"""

from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

csv_file_path = Path(__file__).parents[1] / "resources" / "foo.csv"


frame = (spark
         .read
         .options(header="true", sep=";")
         .csv(str(csv_file_path))
         )

frame.printSchema()

better_frame = frame.withColumn("Date_of_Birth", F.to_date(F.col("Date_of_Birth")))
better_frame.printSchema()
better_frame.repartition(3).write.parquet(str(csv_file_path.parent / "as_parquet"))
