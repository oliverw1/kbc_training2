import datetime

import holidays
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofweek, udf
from pyspark.sql.types import BooleanType, DateType, StructField, StructType


def label_weekend(frame, colname="date"):
    return frame.withColumn(
        "is_weekend",
        dayofweek(col(colname)).isin(1, 7))


def is_belgian_holiday(date: datetime.date):
    belgian_holidays = holidays.BE()
    return date in belgian_holidays


def label_holidays(frame: DataFrame, colname: str = "date") -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""
    holiday_udf = udf(lambda z: is_belgian_holiday(z), BooleanType())
    # Note: using lambdas for the sole purpose of delegating to a
    # single-argument function is silly. Just pass in the reference to the
    # original function instead. Here, I'm using it to show that this is smt
    # you encounter frequently with both beginners and intermediate level
    # programmers.

    return frame.withColumn(
        "is_belgian_holiday",
        holiday_udf(col(colname)))


def label_holidays2(frame: DataFrame, colname: str = "date") -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""

    # A more efficient implementation of `label_holidays`. Major downside is
    # that the range of years needs to be known a priori. Put them in a config
    # file or extract the range from the data beforehand.
    holidays_be = holidays.BE(years=list(range(2015, 2020)))
    return frame.withColumn(
        "is_belgian_holiday",
        col(colname)).isin(list(holidays_be.keys()))


def label_holidays3(frame: DataFrame, colname: str = "date") -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""

    # Another more efficient implementation of `label_holidays`. Same downsides
    # as label_holidays2, but scales better.
    holidays_be = holidays.BE(years=list(range(2015, 2020)))
    spark = SparkSession.builder.getOrCreate()

    holidays_frame = spark.createDataFrame(
        data=[(_, True) for _ in holidays_be.keys()],
        schema=StructType([
            StructField("date", DateType(), False),
            StructField("is_belgian_holiday", BooleanType(), False)
        ]))

    return (frame
            .join(holidays_frame, on="date", how="left")
            .na.fill(False, ["is_belgian_holiday"]))
