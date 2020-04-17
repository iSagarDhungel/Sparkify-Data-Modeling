import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')
#os.environ["AWS_ACCESS"] = config['AWS']['AWS_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates sparks session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    process all song from json files from input_data and writes it to output_data directory
    
    args:
    inputdata = input data directory (s3)
    outputdata = output data directory (s3)
    spark = sprak session object
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/A/B/C/TRABCEI128F424C983.json")
    #song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    # read song data file
    df = spark.read.json(song_data)
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """ 
    Process all logs of sparkfy app usuage; filters by NextSong
    
    args:
    inputdata = input data directory (s3)
    outputdata = output data directory (s3)
    spark = sprak session object
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/'

    # read log data file
    print('Reading in log data')
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table   
    artists_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # write users table to parquet files
    artists_table.write.parquet(output_data+'/users_table')

    # create timestamp column from original timestamp column
    print('Creating time table')
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    df = df.withColumn('hour', F.hour(df.datetime))
    df = df.withColumn('day', F.dayofmonth(df.datetime))
    df = df.withColumn('week', F.weekofyear(df.datetime))
    df = df.withColumn('month', F.month(df.datetime))
    df = df.withColumn('year', F.year(df.datetime))
    df = df.withColumn('weekday', F.dayofweek(df.datetime))
    
    # extract columns to create time table
    time_table = df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+'/timetable')

    # read in song data to use for songplays table
    print('Creating songplays table')
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist)
    songplays_table = songplays_table.withColumn("songplay_id",F.monotonically_increasing_id())
    songplays_table = songplays_table[['songplay_id', 'start_time', 'userId', 'level', 'song_id', 
                                      'artist_id', 'sessionId', 'location', 'userAgent','month','year']]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + '/songplays_table')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://buckudacity/data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
