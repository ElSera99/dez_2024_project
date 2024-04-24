import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import pyspark
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

def etl_samples(version, credentials_location):
    # Set Spark Session
    conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.driver.memory", "16g")

    sc = SparkContext(conf=conf)

    spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

    # Read files

    # Transform Samples
    df_samples_join_1 = df_sample_data.filter(df_sample_data.is_key_frame == True).join(df_sample, df_sample_data.sample_token == df_sample.token, 'inner').drop('token')
    df_samples_join_2 = df_samples_join_1.join(df_scene, df_samples_join_1.scene_token == df_scene.token, 'inner').drop('token')
    df_samples_join_3 = df_samples_join_2.join(df_log, df_samples_join_2.log_token == df_log.token, 'inner').drop('token')
    samples = df_samples_join_3.select('sample_token','logfile','vehicle', 'location', 'scene_name', 'description', 'timestamp_sample_data','ego_pose_token')

    # Write samples
    samples.write.partitionBy("vehicle").mode('overwrite').parquet("gold/samples/")

    spark.stop()


def etl_objects(version, credentials_location):
    # Set Spark Session
    conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.driver.memory", "16g")

    sc = SparkContext(conf=conf)

    spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

    # Read files
    df_category = spark.read.parquet("silver/seeds/category.parquet")
    df_visibility = spark.read.parquet("silver/seeds/visibility.parquet")
    
    # Transform Objects
    df_objects_join_1 = df_sample_annotation.join(df_instance, df_sample_annotation.instance_token == df_instance.token,'inner').drop('token', 'instance_token')
    df_objects_join_2 = df_objects_join_1.join(df_category, df_objects_join_1.category_token == df_category.uid, 'inner').withColumnRenamed('description', 'category_description').drop('uid', 'category_token')
    objects = df_objects_join_2.join(df_visibility, df_objects_join_2.visibility_token == df_visibility.uid, 'inner').drop('uid', 'visibility_token', 'description', 'attribute_tokens')

    # Write Objects
    objects.write.partitionBy("object").mode('overwrite').parquet("gold/objects/")

    spark.stop()

if __name__ == "__main__":
    version = "v1.0"
    folders = ["test", "trainval"]
    sets = ["test_meta","trainval_meta"]

    credentials_location = os.getenv['GOOGLE_APPLICATION_CREDENTIALS']

    etl_seeds(version, folders[0], sets[0])
    etl_sources(version, credentials_location)