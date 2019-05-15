import pyspark.sql.functions as fun
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_catalog = {"foo": "some_path"}

spark = SparkSession.builder.getOrCreate()

tv_sessions = spark.read.parquet(data_catalog['foo  '])

tv_sessions = (tv_sessions
               .filter((tv_sessions.year == 2018)
                       & (tv_sessions.month >= 1)
                       & (tv_sessions.mediaType == 'series-videos')
                       & (tv_sessions.reach60Srd == 1))
               .filter(tv_sessions.source.isin('box', 'corder'))
               .select('customerNumber',
                       'sessionRecordStartTime',
                       'programGenreLevel',
                       'programSeriesName',
                       'programSeasonEpisode')
               .withColumn("date",
                           fun.from_unixtime(fun.unix_timestamp(tv_sessions.sessionRecordStartTime), "yyyy-MM-dd"))
               )

# tv_sessions.sessionRecordStartTime is a _timestamp_ type.

import pyspark.sql.functions as sparkfun


def substring_f(startpos, lengte):
    return sparkfun.udf(lambda kolom: kolom[startpos - 1:startpos - 1 + lengte])


tv_sessions = (tv_sessions
               .withColumn("kijkmaand", substring_f(startpos=6, lengte=2)(tv_sessions.date))
               .withColumn("hoofdgenre", substring_f(startpos=1, lengte=3)(tv_sessions.programGenreLevel))
               .withColumn("season", substring_f(startpos=16, lengte=2)(tv_sessions.programSeasonEpisode))
               .withColumn("episode", substring_f(startpos=19, lengte=4)(tv_sessions.programSeasonEpisode))
               )

tv_sessions = (tv_sessions
               .filter(tv_sessions.kijkmaand.isin('01', '02', '03', '04', '05', '06'))
               .filter((tv_sessions.hoofdgenre == '13.')
                       & (tv_sessions.programGenreLevel != '13.6')))  # geen soaps

### When did you see the first episode of a season of a series
firstepisodes = (tv_sessions.where(tv_sessions.episode == '0001')
                 .select(col("customerNumber"),
                         col("programSeriesName"),
                         col("sessionRecordStartTime").alias("first_sessionRecordStartTime"),
                         col("date").alias("first_date"),
                         col("season")))

### What was the last episode of a season of a series that he saw
maxepisodes = (tv_sessions
               .groupBy('customerNumber', 'programSeriesName', 'season')
               .agg(fun.max(tv_sessions.episode).alias('max_episode'))
               )

maxdate = (tv_sessions
           .join(maxepisodes,
                 (tv_sessions('customerNumber') == maxepisodes('customerNumber'))
                 & (tv_sessions('programSeriesName') == maxepisodes('programSeriesName'))
                 & (tv_sessions('season') == maxepisodes('season'))
                 & (tv_sessions("episode") == maxepisodes("max_episode")),
                 "inner")

           .select(tv_sessions("customerNumber"),
                   tv_sessions("programSeriesName"),
                   tv_sessions("season"),
                   maxepisodes("max_episode"),
                   tv_sessions("sessionRecordStartTime").alias("last_sessionRecordStartTime"),
                   tv_sessions("last_date").alias("last_date"))
           )

samen = (firstepisodes
         .join(maxdate,
               ["customerNumber", "programSeriesName", "season"],
               "left outer")
         .drop("season"))

samenfiltered = samen.withColumn('days_spread', fun.datediff(samen.last_sessionRecordStartTime,
                                                             samen.first_sessionRecordStartTime))
samenfiltered = samenfiltered.where(samenfiltered.days_spread <= 7)

samenfiltered.cache()
samenfiltered.take(2)
samenfiltered.write.parquet('bingewatchers')
