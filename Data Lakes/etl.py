import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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
    song_data = f'{input_data}/song-data/A/A/A/*.json' # smaller subsection
    #song_data = f'{input_data}/song-data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (df.select('song_id','title', 'artist_id', 'year', 'duration').distinct())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = (
        df.select(
            'artist_id',
            col('artist_name'),
            col('artist_location'),
            col('artist_latitude'),
            col('artist_longitude')
        ).distinct())
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+ "artists.parquet", mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    """ 
    Process all logs of sparkfy app usuage; filters by NextSong
    
    args:
    inputdata = input data directory (s3)
    outputdata = output data directory (s3)
    spark = sprak session object
    """

    # get filepath to log data file
    log_data = input_data + '/log_data/A/A/A/*.json'

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong").cache()

    # extract columns for users table
    user_table = df.select(col("firstName"), col("lastName"), col("gender"), col("level"), col("userId")).distinct()

    # write users table to parquet files
    user_table.write.parquet("{}users/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))

    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))

    time_table = df.select(col("start_time"), col("hour"), col("day"), col("week"), \
                           col("month"), col("year"), col("weekday")).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet("{}time/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner").distinct() \
        .select(col("start_time"), col("userId"), col("level"), col("sessionId"), col("location"), col("userAgent"), col("song_id"), col("artist_id")).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet("{}songplays/songplays_table.parquet".format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://buckudacity/data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
