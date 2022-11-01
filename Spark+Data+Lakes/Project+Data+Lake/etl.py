import configparser
from datetime import datetime
from pyspark.sql.functions import dayofweek
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# In case you want to run this code locally, please set these next lines as comment

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

# In case you want to run this code locally, please set these above lines as comment

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the song data from S3, processes that data using Spark, and writes them back to S3

    Parameters
    ----------
        spark: This is the spark session
        input_date: This is the path to song_date into S3 bucket
        output_data: This is the path where parquet files will be written
    
    """
    
    # get filepath to song data file
    # e.g. from S3 file location: song_data/A/B/C/TRABCEI128F424C983.json
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude').dropDuplicates()
    
    artists_table.createOrReplaceTempView('artists')
    
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This function reads the log data from S3, processes that data using Spark, and writes them back to S3

    Parameters
    ----------
        spark: This is the spark session
        input_date: This is the path to log_date into S3 bucket
        output_data: This is the path where parquet files will be written
    
    """
    
    
    # get filepath to log data file
    # e.g. from S3 file location: log_data/2018/11/2018-11-12-events.json
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # df_actions = df.filter(df.page == "NextSong").select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent')
    df_actions = df.where(df.page == 'NextSong')
    df_actions.select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent')
    
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df_actions.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df_actions.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                           .withColumn('start_time', df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime')) \
                           .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    df = df.alias('log_df')
    song_df = song_df.alias('song_df')
    log_and_songs = df.join(song_df, col('log_df.artist') == col('song_df.artist_name'), 'inner')
    
    
    songplays_table = log_and_songs.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month'))

    songplays_table.createOrReplaceTempView('songplays')
    
    # write songplays table to parquet files partitioned by year and month
    time_table = time_table.alias('timetable')

    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')


def main():
    '''Start spark session and define S3 locations where the files will be readed and written'''
    spark = create_spark_session()
    
    '''Paths to run on S3 buckets'''
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://gp-output-data"
    
    '''Paths for local testing'''
#     input_data = "data/"
#     output_data = "data/outputs/"
    
    
    '''Run above functions according to the previous parameters '''
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
