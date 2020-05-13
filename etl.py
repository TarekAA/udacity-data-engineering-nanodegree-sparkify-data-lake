import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']] \
    .withColumn('year', df["year"].cast(IntegerType())) \
    .withColumn('duration', df['duration'].cast(DecimalType()))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite') \
    .parquet(output_data + 'songs')

    # extract columns to create artists table
    # todo from nb didn't work remove
#     artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']] \
#     .withColumn('artist_latitude', df["artist_latitude"].cast(DecimalType())) \
#     .withColumn('artist_longitude', df['artist_longitude'].cast(DecimalType())) \
#     .selectExpr('artist_id', 'artist_name as name', 'artist_location as location',
#             'artist_latitude as latitude', 'artist_longitude as longitude')
    artists_table = df.withColumn('artist_latitude', df["artist_latitude"].cast(DecimalType())) \
    .withColumn('artist_longitude', df['artist_longitude'].cast(DecimalType())) \
    .selectExpr('artist_id', 'artist_name as name', 'artist_location as location',
            'artist_latitude as latitude', 'artist_longitude as longitude')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
