# Exercise:
# “clean” a CSV file using pyspark.
# * Grab the US flight data of the year 2000 from
#   [the website of the American Statistical Association](http://stat-computing.org/dataexpo/2009/the-data.html).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with pyspark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
# * Write the data away as a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor)
