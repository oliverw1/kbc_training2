from datetime import date

from pyspark.sql import SparkSession

from exercises.catalog.simple_catalog import catalog, load_frame_from_catalog


def main(execution_date: date):
    spark = SparkSession.builder.getOrCreate()
    df = load_frame_from_catalog(spark, catalog["raw_airports"])

    # no cleansing actions are required. Store it as parquet though.

    (df.write
     .mode("overwrite")
     .format(catalog["clean_airports"].format)
     .save(str(catalog["clean_airports"].path))
     )


if __name__ == "__main__":
    main(date.today())
