# Exercise:
# “clean” a CSV file using pyspark.
# * Grab the US flight data of the year 2000 from
#   [the website of the American Statistical Association](http://stat-computing.org/dataexpo/2009/the-data.html).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with pyspark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
# * Write the data away as a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor)

from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession, Column, DataFrame
from pyspark.sql.functions import col, to_date, concat_ws, when
from pyspark.sql.types import *


def hhmm_to_minutes_since_midnight(mycol: Column) -> Column:
    # Using math to do this conversion is _probably_ more efficient than using
    # string manipulations.
    minutes = mycol % 100
    hours = (mycol - minutes) / 100
    return (minutes + hours * 60).cast(ShortType())


def fix_NA_placeholders(frame: DataFrame, columns: Iterable[str]) -> DataFrame:
    for c in columns:
        frame = frame.withColumn(c, fix_null_placeholder(c, "NA"))
    return frame


def fix_null_placeholder(colname: str, placeholder: str) -> Column:
    return when(col(colname) == placeholder, None).otherwise(col(colname))


def read_data(path: Path, session: SparkSession):
    return (session.read
            # For a CSV, `inferschema=False` means every column stays of the string
            # type. There is no time wasted on inferring the schema, which is
            # arguably not something you would depend on in production either.
            .option("inferschema", "false")
            .option("header", "true")
            .csv(str(path)))


def clean(frame: DataFrame) -> DataFrame:
    # First, get the majority of columns “fixed”, i.e. their datatypes improved.
    df2 = (frame
           .withColumn('Year', col('Year').cast(ShortType()))
           .withColumn('Month', col('Month').cast(ByteType()))
           .withColumn('DayofMonth', col('DayofMonth').cast(ByteType()))
           .withColumn('DayOfWeek', col('DayOfWeek').cast(ByteType()))
           .withColumn('DepTime', col('DepTime').cast(ShortType()))  # TODO
           .withColumn('CRSDepTime', col('CRSDepTime').cast(ShortType()))  #
           .withColumn('ArrTime', col('ArrTime').cast(ShortType()))
           .withColumn('CRSArrTime', col('CRSArrTime').cast(ShortType()))
           # .withColumn('UniqueCarrier', col('UniqueCarrier').cast(StringType()))
           .withColumn('FlightNum', col('FlightNum').cast(ShortType()))
           .withColumn('TailNum', col('TailNum').cast(IntegerType()))
           .withColumn('ActualElapsedTime', col('ActualElapsedTime').cast(ShortType()))
           .withColumn('CRSElapsedTime', col('CRSElapsedTime').cast(ShortType()))
           .withColumn('AirTime', col('AirTime').cast(ShortType()))
           .withColumn('ArrDelay', col('ArrDelay').cast(ShortType()))
           .withColumn('DepDelay', col('DepDelay').cast(ShortType()))
           .withColumn('Origin', col('Origin').cast(StringType()))
           .withColumn('Dest', col('Dest').cast(StringType()))
           .withColumn('Distance', col('Distance').cast(IntegerType()))  # Short
           .withColumn('TaxiIn', col('TaxiIn').cast(ShortType()))
           .withColumn('TaxiOut', col('TaxiOut').cast(ShortType()))
           .withColumn('Cancelled', col('Cancelled').cast(BooleanType()))
           .withColumn('CancellationCode', col('CancellationCode').cast(StringType()))
           .withColumn('Diverted', col('Diverted').cast(BooleanType()))
           .withColumn('CarrierDelay', col('CarrierDelay').cast(ShortType()))
           .withColumn('WeatherDelay', col('WeatherDelay').cast(ShortType()))
           .withColumn('NASDelay', col('NASDelay').cast(ShortType()))
           .withColumn('SecurityDelay', col('SecurityDelay').cast(ShortType()))
           .withColumn('LateAircraftDelay', col('LateAircraftDelay').cast(ShortType()))
           )
    # The solution above is good. However, there's a lot of repetition and it's not
    # easy to find out which columns are somewhat similar.
    # One way to improve this, would be to group columns of similar types in a
    # dictionary and use a simple looping construct to do all that `withColumn`
    # stuff shorter. Like this:
    #
    # mapping = {
    #        "tinyint": {"DayofMonth", "DayOfWeek", …}
    #        "short": {"CarrierDelay", "WeatherDelay", …},
    #        "int": {…},
    #        "bool": {…}
    # }
    # for datatype, colnames in mapping:
    #        for colname in colnames:
    #               frame = frame.withColumn(colname, col(colname).cast(datatype))
    #
    # This looping construct is so easy to use, it should probably be in its own
    # function. Which of course you test 😉. You'll re-use it quite often.

    # Now, the columns that couldn't be dealt with by a mere “cast” can get
    # special care:
    df3 = (df2.withColumn("Date",
                          to_date(concat_ws("-", col("Year"), col("Month"),
                                            col("DayOfMonth"))))
           .drop("Year", "Month", "DayOfMonth", "DayOfWeek"))

    df4 = (df3
           .withColumn("DepTime", hhmm_to_minutes_since_midnight(col("DepTime")))
           .withColumn("CRSDepTime", hhmm_to_minutes_since_midnight(col("CRSDepTime")))
           .withColumn("ArrTime", hhmm_to_minutes_since_midnight(col("ArrTime")))
           .withColumn("CRSArrTime", hhmm_to_minutes_since_midnight(col("CRSArrTime")))
           )
    # Again, repetition for these last 4 columns. It would be less copy-pasting,
    # if you simply use a loop. Like this:
    # for colname in {"DepTime", "CRSDepTime", "ArrTime", "CRSArrTime"}:
    #     df4 = df4.withColumn(colname, hhmm_to_minutes_since_midnight(col(colname)))
    #
    # If you were interested in simply fixing the NA placeholders and not perform
    # any casting to numeric types (which will result in null for any non-numeric
    # item anyway), then you could go about it in this way:
    # cols_with_NA_placeholders = {"WeatherDelay",
    #                              "CancellationCode",
    #                              "CarrierDelay",
    #                              "NASDelay",
    #                              "SecurityDelay",
    #                              "LateAircraftDelay"}
    # df4 = fix_NA_placeholders(df3, cols_with_NA_placeholders)
    return df4


if __name__ == "__main__":
    resources_dir = Path(__file__).parents[1] / "resources"
    target_dir = Path(__file__).parents[1] / "target"
    target_dir.mkdir(exist_ok=True)

    spark = SparkSession.builder.getOrCreate()
    # Extract
    frame = read_data(resources_dir / "flights" / "2008.csv", spark)
    # Transform
    cleaned_frame = clean(frame)
    # Load
    cleaned_frame.write.mode("overwrite").parquet(str(target_dir / "cleaned_flights"))
