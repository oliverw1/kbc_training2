from datetime import date

from pyspark.sql import SparkSession

from exercises.catalog.catalog import catalog, load_frame_from_catalog


def main(execution_date: date):
    spark = SparkSession.builder.getOrCreate()
    df = load_frame_from_catalog(spark, catalog["raw_carriers"])

    # no cleansing actions are required. Store it as parquet though.

    (df.write
     .mode("overwrite")
     .format(catalog["clean_carriers"].format)
     .save(str(catalog["clean_carriers"].path)))


if __name__ == "__main__":
    main(date.today())
