from pyspark.sql import DataFrame


def assert_frames_functionally_equivalent(frame1: DataFrame, frame2: DataFrame):
    """Validate if 2 frames have identical schemas, and data, disregarding the
    ordering of both.
    """
    pass
