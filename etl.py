import configparser
from datetime import datetime as dt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a Spark session
    used "getOrCreate" incase the instance has already been initialized"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_path, output_data_path):
    """opens the song data file
    extracts the data to create the songs_table and artists_table
    spark: spark session
    input_data_path: input file path
    output_data_path: output file path
    """
    # get filepath to song data file
    song_data_path = os.path.join(input_data_path, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data_path)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data_path, 'songs', mode='overwrite', partitionBy=['year','artist_id']))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data_path, 'artists', mode='overwrite'))



def process_log_data(spark, input_data_path, output_data_path):
    """opens the log data files
    extracts data to create the users, time and songplays tables.
    spark: spark session
    input_data_path: input file path
    output_data_path: output file path
    """
    # get filepath to log data file
    log_data_path = os.path.join(input_data_path, 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
                        col('userId').alias('user_id'),
                        col('firstName').alias('first_name'),
                        col('lastName').alias('last_name'),
                        'gender',
                        'level') \
                    .drop_duplicates(subset=['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data_path, 'users'), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(dt.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.withColumn('hour', hour('datetime')) \
                   .withColumn('day', dayofmonth('datetime')) \
                   .withColumn('week', weekofyear('datetime')) \
                   .withColumn('month', month('datetime')) \
                   .withColumn('year', year('datetime')) \
                   .withColumn('weekday', dayofweek('datetime')) \
                   .select(date_format(from_unixtime('timestamp'), 'h:m:s').alias('start_time'), \
                           'hour', 'day', 'week', 'month', 'year', 'weekday') \
                   .drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data_path, 'time'), mode='overwrite', partitionBy=['year', 'month'])

    # get filepath to log data file
    song_data_path = os.path.join(input_data_path, 'song_data/*/*/*/*.json')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data_path)

    # extract columns from joined song and log datasets to create songplays table 
    conditions = [df.song == song_df.title, df.artist == song_df.artist_name, df.length == song_df.duration]
    songplays_table = df.join(song_df, conditions)

    songplays_columns = ['start_time', 'userId as user_id', 'level', 'song_id','artist_id', 'sessionId as session_id','artist_location as location', 'userAgent as user_agent']

    songplays_table = (songplays_table.selectExpr(songplays_columns)
                       .withColumn('month', month('start_time'))
                       .withColumn('year', year('start_time')))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data_path, 'songplays'), mode='overwrite', partitionBy=['year', 'month'])

def main():
    spark = create_spark_session()
    input_data_path = "s3a://udacity-dend/"
    output_data_path = ""
    
    process_song_data(spark, input_data_path, output_data_path)    
    process_log_data(spark, input_data_path, output_data_path)


if __name__ == "__main__":
    main()
