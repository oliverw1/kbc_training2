from pyspark.sql.functions import dayofweek, col

def label_weekend(frame, colname="date"):
    return frame.withColumn(
        "is_weekend",
        dayofweek(col(colname)).isin(1, 7))

