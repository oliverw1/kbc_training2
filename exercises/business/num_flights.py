from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, avg, when, sum
from pyspark.sql.types import ByteType
from exercises.catalog.catalog import load_frame_from_catalog, catalog

spark = SparkSession.builder.getOrCreate()

frame = load_frame_from_catalog(spark, catalog["master_flights"])
american_airlines_flights = (frame
                             .filter(col("CarrierName").like("%American Airlines%"))
                             .cache())

print("Number of flights operated by American Airlines in 2008: {}"
      .format(american_airlines_flights.count()))
print("Of those, {} arrived less than (or equal to) 10 minutes later"
      .format(american_airlines_flights.filter(col("ArrDelay") <= 10).count()))


p = (frame
 .withColumn("has_delayed_departure", when(col("DepDelay") > 0, 1).otherwise(0))
 .groupBy(dayofweek("Date").alias("weekday"))
 .agg(avg("has_delayed_departure"))).collect()
from pprint import pprint

print("The average number of delayed departures, sorted by weekday (1 = Su, 7=Sa):")
pprint(sorted(p))
print("The largest number here is for:")
print(max(p, key=lambda x: x[1]))
print("which confirms the data scientist's hunch")

delay_categories = "CarrierDelay WeatherDelay NASDelay SecurityDelay LateAircraftDelay".split(" ")
frame = american_airlines_flights.select(delay_categories)

expressions = [(sum((col(_) > 0).cast(ByteType()))
                    .alias("pos_{}_count".format(_)))
               for _ in delay_categories]
# Note: the ability to _generate_ SQL expressions like this, with a simple for
# loop is incredibly useful, as there are less chances for errors and you can
# keep the amount of lines very small.
frame.agg(*expressions).show()
print("The pos_NASDelay_count shows that NASDelay should be focussed on")
# This is of course assuming the incidents get labelled without bias. It
# is perfectly possible for ground personel to be fed up with a certain kind of
# problem and therefore flag those more often.