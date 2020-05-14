import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a spark session with the default parameters.
    SparkSession is the entry point to interact with DataFrame and Dataset APIs.

    :return: SparkSession, a configured SparkSession
    """
    spark = SparkSession \
        .builder \
        .appName('etl') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data residing in s3. Song data is a collection of JSON files containing description
    of songs. Data come from the Million Song Data set.
    It's partitioned by the first Three letters of each song track id.

    Data is loaded from s3. Processed into a dimensional model.
    Then loaded back to s3 as partitioned parquet files.

    :param spark: SparkSession
    :param input_data: string, s3a formatted uri to the bucket containing input data
    :param output_data: string, s3a formatted uri to a bucket where data can be saved to
    :return: None
    """
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
    artists_table = df.withColumn('artist_latitude', df["artist_latitude"].cast(DecimalType())) \
        .withColumn('artist_longitude', df['artist_longitude'].cast(DecimalType())) \
        .selectExpr('artist_id', 'artist_name as name', 'artist_location as location',
                    'artist_latitude as latitude', 'artist_longitude as longitude')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """Process log data residing in s3. log data is a collection of JSON files containing description
    of user events. Data come from an event simulator.
    It's partitioned by year and month.

    Data is loaded from s3. Processed into a dimensional model.
    Then loaded back to s3 as partitioned parquet files.

    :param spark: SparkSession
    :param input_data: string, s3a formatted uri to the bucket containing input data
    :param output_data: string, s3a formatted uri to a bucket where data can be saved to
    :return: None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.withColumn('userId', df["userId"].cast(IntegerType())) \
        .selectExpr('userId as user_id', 'firstName as first_name',
                    'lastName as last_name', 'gender', 'level')

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts / 1000, LongType())
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000), TimestampType())
    df = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select(df['timestamp'].alias('start_time'),
                           hour('datetime').alias('hour'), dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'), month('datetime').alias('month'),
                           year('datetime').alias('year'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title) \
        .select(monotonically_increasing_id().alias('songplay_id'), df.timestamp.alias('start_time'),
                df.userId.alias('user_id'), 'level', song_df.song_id, song_df.artist_id,
                df.sessionId.alias('session_id'), df.location, df.userAgent.alias('user_agent'), df.ts)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn('year', year(get_datetime('ts').cast(StringType()))) \
        .withColumn('month', month(get_datetime('ts').cast(StringType())))

    songplays_table.drop('ts').write.partitionBy('year', 'month').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
