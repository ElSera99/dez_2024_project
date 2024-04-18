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


def etl_seeds(version,folder,sets):
# Modify seeds
    df_category_pd = pd.read_json(f"bronze/{version}-{sets}/{version}-{folder}/category.json")
    df_category_pd['object'] = df_category_pd['name'].apply(lambda x: x.split('.')[-1])
    df_category_pd.rename(columns={'token':'uid', 'name':'category'},inplace=True)
    df_category_pd.astype({'uid':'string', 'category':'string', 'description':'string', 'object':'string'},copy=False)

    df_visibility_pd = pd.read_json(f"bronze/{version}-{sets}/{version}-{folder}/visibility.json")
    df_visibility_pd.drop(columns=['level'],inplace=True)
    low = [0,40,60,80]
    high = [40,60,80,100]
    df_visibility_pd['level_low'], df_visibility_pd['level_high'] = low, high
    df_visibility_pd.rename(columns={'token':'uid'}, inplace=True)
    df_visibility_pd.astype({'uid':'string', 'description':'string', 'level_low':'int64', 'level_high': 'int64'}, copy=False)

    # Store seeds
    category = pa.Table.from_pandas(df_category_pd)
    pq.write_table(category, 'silver/seeds/category.parquet')
    visibility = pa.Table.from_pandas(df_visibility_pd)
    pq.write_table(visibility, 'silver/seeds/visibility.parquet')


def etl_sources(version,credentials_location):
    # Set Spark Session
    conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.driver.memory", "16g") # Select amount memory to use by driver
    
    sc = SparkContext(conf=conf)

    # hadoop_conf = sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    # hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    # hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    # hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

    # Schemas
    schema_log = types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('logfile', types.StringType(), True),
    types.StructField('vehicle', types.StringType(), True),
    types.StructField('date_captured', types.DateType(), True),
    types.StructField('location', types.StringType(), True)
    ])

    schema_scene = types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('log_token', types.StringType(), True),
    types.StructField('nbr_samples', types.IntegerType(), True),
    types.StructField('first_sample_token', types.StringType(), True),
    types.StructField('last_sample_token', types.StringType(), True),
    types.StructField('name', types.StringType(), True),
    types.StructField('description', types.StringType(), True)
    ])

    schema_sample = types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('timestamp', types.TimestampType(), True),
    types.StructField('prev', types.StringType(), True),
    types.StructField('next', types.StringType(), True),
    types.StructField('scene_token', types.StringType(), True)
    ])

    schema_sample_data = types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('sample_token', types.StringType(), True),
    types.StructField('ego_pose_token', types.StringType(), True),
    types.StructField('calibrated_sensor_token', types.StringType(), True),
    types.StructField('timestamp', types.TimestampType(), True),
    types.StructField('fileformat', types.StringType(), True),
    types.StructField('is_key_frame', types.BooleanType(), True),
    types.StructField('height', types.IntegerType(), True),
    types.StructField('width', types.IntegerType(), True),
    types.StructField('filename', types.StringType(), True),
    types.StructField('prev', types.StringType(), True),
    types.StructField('next', types.StringType(), True)
    ])

    schema_sample_annotation = types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('sample_token', types.StringType(), True),
    types.StructField('instance_token', types.StringType(), True),
    types.StructField('visibility_token', types.DecimalType(), True),
    types.StructField('attribute_tokens', types.StringType(), True),
    types.StructField('translation', types.StringType(), True),
    types.StructField('size', types.StringType(), True),
    types.StructField('rotation', types.StringType(), True),
    types.StructField('prev', types.StringType(), True),
    types.StructField('next', types.StringType(), True),
    types.StructField('num_lidar_pts', types.IntegerType(), True),
    types.StructField('num_radar_pts', types.IntegerType(), True)
    ])

    schema_instance = types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('category_token', types.StringType(), True),
    types.StructField('nbr_annotations', types.IntegerType(), True),
    types.StructField('first_annotation_token', types.StringType(), True),
    types.StructField('last_annotation_token', types.StringType(), True)
    ])

    # Read from Bronze - Test
    df_log_test = spark.read.option("multiline", True).schema(schema_log).json(f'bronze/{version}-test_meta/{version}-test/log.json')
    df_scene_test = spark.read.option("multiline", True).schema(schema_scene).json(f'bronze/{version}-test_meta/{version}-test/scene.json')
    df_sample_test = spark.read.option("multiline", True).schema(schema_sample).json(f'bronze/{version}-test_meta/{version}-test/sample.json')
    df_sample_data_test = spark.read.option("multiline", True).schema(schema_sample_data).json(f'bronze/{version}-test_meta/{version}-test/sample_data.json')
    df_sample_annotation_test = spark.read.option("multiline", True).schema(schema_sample_annotation).json(f'bronze/{version}-test_meta/{version}-test/sample_annotation.json')
    df_instance_test = spark.read.option("multiline", True).schema(schema_instance).json(f'bronze/{version}-test_meta/{version}-test/instance.json')
    # Read from Bronze - Trainval
    df_log_trainval = spark.read.option("multiline", True).schema(schema_log).json(f'bronze/{version}-trainval_meta/{version}-trainval/log.json')
    df_scene_trainval = spark.read.option("multiline", True).schema(schema_scene).json(f'bronze/{version}-trainval_meta/{version}-trainval/scene.json')
    df_sample_trainval = spark.read.option("multiline", True).schema(schema_sample).json(f'bronze/{version}-trainval_meta/{version}-trainval/sample.json')
    df_sample_data_trainval = spark.read.option("multiline", True).schema(schema_sample_data).json(f'bronze/{version}-trainval_meta/{version}-trainval/sample_data.json')
    df_sample_annotation_trainval = spark.read.option("multiline", True).schema(schema_sample_annotation).json(f'bronze/{version}-trainval_meta/{version}-trainval/sample_annotation.json')
    df_instance_trainval = spark.read.option("multiline", True).schema(schema_instance).json(f'bronze/{version}-trainval_meta/{version}-trainval/instance.json')

    # Merge data
    df_log = df_log_trainval.union(df_log_test)
    df_scene = df_scene_trainval.union(df_scene_test)
    df_sample = df_sample_trainval.union(df_sample_test)
    df_sample_data = df_sample_data_trainval.union(df_sample_data_test)
    df_sample_annotation = df_sample_annotation_trainval.union(df_sample_annotation_test)
    df_instance = df_instance_trainval.union(df_instance_test)

    # Select data
    df_log = df_log.select('token', 'logfile', 'vehicle','location')
    df_scene = df_scene.select('token','log_token','name','description')
    df_sample = df_sample.select('token', 'timestamp','scene_token')
    df_sample_data = df_sample_data.select('sample_token','ego_pose_token','timestamp','is_key_frame','filename')
    df_sample_annotation = df_sample_annotation.select('sample_token','instance_token','visibility_token','attribute_tokens')
    df_instance = df_instance.select('token','category_token','nbr_annotations')

    # Store data
    df_log.write.partitionBy("vehicle").mode('overwrite').parquet("silver/sources/log/")
    df_scene.write.partitionBy("name").mode('overwrite').parquet("silver/sources/scene/")
    df_sample.write.mode('overwrite').parquet("silver/sources/sample/")
    df_sample_data.coalesce(10).write.mode('overwrite').parquet("silver/sources/sample_data/")
    df_sample_annotation.write.mode('overwrite').parquet("silver/sources/sample_annotation/")
    df_instance.write.partitionBy("category_token").mode('overwrite').parquet("silver/sources/instance/")


if __name__ == "__main__":
    version = "v1.0"
    folders = ["test", "trainval"]
    sets = ["test_meta","trainval_meta"]

    credentials_location = os.getenv['GOOGLE_APPLICATION_CREDENTIALS']

    etl_seeds(version, folders[0], sets[0])
    etl_sources(version, credentials_location)