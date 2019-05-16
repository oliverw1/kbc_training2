from pathlib import Path
from typing import NamedTuple, Dict

from pyspark.sql import DataFrame, SparkSession

RESOURCES_DIR = Path(__file__).parents[1] / "resources"
TARGET_DIR = Path(__file__).parents[1] / "target"
TARGET_DIR.mkdir(exist_ok=True)


class DataLink(NamedTuple):
    format: str
    path: str
    options: Dict[str, str]


csv_options = {"header": "true", "inferschema": "false"}
catalog = {
    "raw_flights": DataLink("csv",
                            RESOURCES_DIR / "flights" / "2008.csv",
                            csv_options),
    "raw_airports": DataLink("csv",
                             RESOURCES_DIR / "flights" / "airports.csv",
                             csv_options),
    "raw_carriers": DataLink("csv",
                             RESOURCES_DIR / "flights" / "carriers.csv",
                             csv_options),
    "clean_flights": DataLink("parquet", TARGET_DIR / "cleaned_flights", {}),
    "clean_airports": DataLink("parquet", TARGET_DIR / "clean_airports", {}),
    "clean_carriers": DataLink("parquet", TARGET_DIR / "clean_carriers", {}),
    "master_flights": DataLink("parquet", TARGET_DIR / "master_flights", {})
}


def load_frame_from_catalog(spark: SparkSession, link: DataLink) -> DataFrame:
    return (spark.read
            .options(**link.options)
            .format(link.format)
            .load(str(link.path))
            )
