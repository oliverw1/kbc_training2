import logging

from pyspark.sql import DataFrame


def assert_frames_functionally_equivalent(df1: DataFrame, df2: DataFrame,
                                          check_nullability=True):
    """Validate if 2 frames have identical schemas, and data, disregarding the
    ordering of both."""

    try:
        if check_nullability:
            assert set(df1.schema.fields) == set(df2.schema.fields)
        else:
            assert set(df1.dtypes) == set(df2.dtypes)
    except AssertionError:
        logging.warning(df1.schema)
        logging.warning(df2.schema)
        raise
    # Change ordering of columns, then sort the results

    df2_column_sorted = df2.select(df1.columns)
    df3 = df2_column_sorted.orderBy(*df1.columns)
    assert (df1.orderBy(*df1.columns).collect()
            == df3.collect())