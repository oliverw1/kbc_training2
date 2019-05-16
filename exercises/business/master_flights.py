from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_extract, year

from exercises.catalog.catalog import catalog, load_frame_from_catalog


def extract_carrier_operational_period(carriers, colname="Description") -> DataFrame:
    regex = r'([^(]+)\s+\(([12]\d{3}) -\s*([^\)]*)\)'
    for group_index, better_colname, dtype in ((1, "CarrierName", "string"),
                                               (2, "OperationsStartYear", "smallint"),
                                               (3, "OperationsEndYear", "smallint")):
        carriers = carriers.withColumn(
            better_colname,
            regexp_extract(col(colname), regex, group_index).cast(dtype)
        )
    return carriers


def main(spark: SparkSession):
    flights, airports, carriers = (load_frame_from_catalog(spark, catalog[_])
                                   for _ in ("clean_flights", "clean_airports", "clean_carriers"))

    # Remark: the carriers file, even though cleaned, has duplicate keys. When you look at e.g.
    # the code "TB", you'll see it belonged to 3 different organizations over time.
    # If you were to join it with the flights facts table, you'd get an increase in the amount
    # of records, which does not make sense: a flight in 2008 was operated by only one carrier.
    # The correct solution would be to join the carriers who were active in 2008 to the flights
    # data of 2008. That's a bit more involved, as the operational start and end year of an
    # organization are captured within the description column and not made available as separate
    # columns. As an example, it is demonstrated here how to extract those fields using
    # regular expressions (an advanced topic), but the step could (and probably should) be done
    # in the cleansing stage of the dataset. The point of the exercise is not to strive for
    # accuracy, but for you to get a feeling of the way DataFrames get joined in Spark SQL,
    # as well as getting aware of some potential issues when joining.
    # Beware that only _good familiarity_ with the dataset would've prevented you from making
    # a mistake here. Again, the exercise is not about correctness (this is not production
    # code), but about learning Spark SQL joins.
    carriers = extract_carrier_operational_period(carriers)

    master_table = (flights
                    .join(airports,
                          on=airports.Code == flights.Origin,
                          how="left")
                    .drop("Code")
                    .withColumnRenamed("Description", "OriginDescription")
                    .join(airports,
                          on=airports.Code == flights.Dest,
                          how="left")
                    .drop("Code")
                    .withColumnRenamed("Description", "DestinationDescription")
                    .join(carriers,
                          on=((carriers.Code == col("UniqueCarrier"))  # if you just wrote this, it's fine for the purpose of the exercise. The following few lines are for correctness.
                              & (carriers.OperationsStartYear <= year(flights.Date))
                              & ((carriers.OperationsEndYear >= year(flights.Date))
                              | (carriers.OperationsEndYear.isNull()))))  # these are the still active carriers
                    )

    master = catalog["master_flights"]
    (master_table
     .write.mode("overwrite")
     .format(master.format)
     .save(str(master.path)))


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    main(spark)
