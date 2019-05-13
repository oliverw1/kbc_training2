from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StructField, StructType, StringType, BooleanType

from exercises.labellers.label_weekend import label_weekend
from .comparers import assert_frames_functionally_equivalent

spark = SparkSession.builder.master("local[*]").getOrCreate()


def test_label_weekend():
    # make sure to explore
    # https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions

    expected = (spark.createDataFrame([
        (date(2018, 5, 12), "a", True),
        (date(2019, 5, 13), "b", False)
    ], schema=StructType([StructField("date", DateType(), True),
                          StructField("foo", StringType(), True),
                          StructField("is_weekend", BooleanType(), True)])))
    frame_in = expected.select("date", "foo")

    actual = label_weekend(frame_in)
    for frame in (frame_in, actual, expected):
        frame.show()
    assert_frames_functionally_equivalent(actual, expected)
