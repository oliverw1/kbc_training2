from pyspark.sql import DataFrame
from pyspark.sql.functions import dayofweek, col


def label_weekend(frame, colname="date"):
    return frame.withColumn(
        "is_weekend",
        dayofweek(col(colname)).isin(1, 7))


def label_holidays(frame: DataFrame, colname: str = "date") -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""
    pass
