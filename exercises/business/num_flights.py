from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from exercises.catalog.simple_catalog import load_frame_from_catalog, catalog

spark = SparkSession.builder.getOrCreate()

frame = load_frame_from_catalog(spark, catalog["master_flights"])
american_airlines_flights = (frame
                             .filter(col("CarrierName").like("%American Airlines%"))
                             .cache())

print("Number of flights operated by American Airlines in 2008: {}"
      .format(american_airlines_flights.count()))
print("Of those, {} arrived less than (or equal to) 10 minutes later"
      .format(american_airlines_flights.filter(col("ArrDelay") <= 10).count()))
