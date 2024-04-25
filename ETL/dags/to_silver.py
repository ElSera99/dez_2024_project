import os

from dotenv import load_dotenv

import pyspark
from pyspark.sql import types
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


def etl_seeds(master,dataset_version,google_application_credentials,gcp_connector_location,bucket_name):
    # Set Spark Session
    conf = SparkConf() \
    .setMaster(master) \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.jars", gcp_connector_location ) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_application_credentials)
    
    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", google_application_credentials)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

     # Schemas
    schema_category= types.StructType([
    types.StructField('token', types.StringType(), True),
    types.StructField('name', types.StringType(), True),
    types.StructField('description', types.StringType(), True)
    ])

    schema_visbility= types.StructType([
    types.StructField('description', types.StringType(), True),
    types.StructField('token', types.StringType(), True),
    types.StructField('level', types.StringType(), True)
    ])

    # Read from Bronze - Test
    df_category = spark.read.option("multiline", True).schema(schema_category).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/category.json')
    df_visibility = spark.read.option("multiline", True).schema(schema_visbility).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/visibility.json')

    # Transform data
    df_category = df_category.withColumn('object', F.split(df_category.name, '\.')) \
                .withColumn('object', F.col('object')[F.size('object') -1]) \
                .withColumnRenamed('token', 'uid') \
                .withColumnRenamed('name', 'category')
    
    lower = [0,40,60,80]
    lower_visibility = spark.createDataFrame([(l,) for l in lower], ['lower_visibility']) \
                        .withColumn("row_idx_lower", F.row_number() \
                        .over(Window.orderBy(F.monotonically_increasing_id())))
    higher = [40,60,80,100]
    higher_visibility = spark.createDataFrame([(l,) for l in higher], ['higher_visibility']) \
                            .withColumn("row_idx_higher", F.row_number() \
                            .over(Window.orderBy(F.monotonically_increasing_id())))
    
    df_visibility = df_visibility.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    
    join_1 = df_visibility.join(lower_visibility, df_visibility.row_idx == lower_visibility.row_idx_lower).drop('row_idx_lower')
    df_visibility = join_1.join(higher_visibility, join_1.row_idx == higher_visibility.row_idx_higher) \
                    .drop('row_idx_higher','row_idx','level') \
                    .withColumnRenamed('token', 'uid')
    
    # Store data
    df_category.repartition(500).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/seeds/category/")
    df_visibility.repartition(500).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/seeds/visibility/")

    spark.stop()


def etl_sources(master,dataset_version,google_application_credentials,gcp_connector_location,bucket_name):
    # Set Spark Session
    conf = SparkConf() \
    .setMaster(master) \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.jars", gcp_connector_location ) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_application_credentials)
    
    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", google_application_credentials)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

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
    df_log_test = spark.read.option("multiline", True).schema(schema_log).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/log.json')
    df_scene_test = spark.read.option("multiline", True).schema(schema_scene).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/scene.json')
    df_sample_test = spark.read.option("multiline", True).schema(schema_sample).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/sample.json')
    df_sample_data_test = spark.read.option("multiline", True).schema(schema_sample_data).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/sample_data.json')
    df_sample_annotation_test = spark.read.option("multiline", True).schema(schema_sample_annotation).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/sample_annotation.json')
    df_instance_test = spark.read.option("multiline", True).schema(schema_instance).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-test_meta/{dataset_version}-test/instance.json')
    # Read from Bronze - Trainval
    df_log_trainval = spark.read.option("multiline", True).schema(schema_log).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-trainval_meta/{dataset_version}-trainval/log.json')
    df_scene_trainval = spark.read.option("multiline", True).schema(schema_scene).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-trainval_meta/{dataset_version}-trainval/scene.json')
    df_sample_trainval = spark.read.option("multiline", True).schema(schema_sample).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-trainval_meta/{dataset_version}-trainval/sample.json')
    df_sample_data_trainval = spark.read.option("multiline", True).schema(schema_sample_data).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-trainval_meta/{dataset_version}-trainval/sample_data.json')
    df_sample_annotation_trainval = spark.read.option("multiline", True).schema(schema_sample_annotation).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-trainval_meta/{dataset_version}-trainval/sample_annotation.json')
    df_instance_trainval = spark.read.option("multiline", True).schema(schema_instance).json(f'gs://{bucket_name}/{dataset_version}/bronze/{dataset_version}-trainval_meta/{dataset_version}-trainval/instance.json')

    # Merge data
    df_log = df_log_trainval.union(df_log_test)
    df_scene = df_scene_trainval.union(df_scene_test)
    df_sample = df_sample_trainval.union(df_sample_test)
    df_sample_data = df_sample_data_trainval.union(df_sample_data_test)
    df_sample_annotation = df_sample_annotation_trainval.union(df_sample_annotation_test)
    df_instance = df_instance_trainval.union(df_instance_test)

    # Select data
    df_log = df_log.select('token', 'logfile', 'vehicle','location')
    df_log.printSchema()
    df_scene = df_scene.select('token','log_token','name','description')
    df_scene.printSchema()
    df_sample = df_sample.select('token', 'timestamp','scene_token')
    df_sample.printSchema()
    df_sample_data = df_sample_data.select('sample_token','ego_pose_token','timestamp','is_key_frame','filename')
    df_sample_data.printSchema()
    df_sample_annotation = df_sample_annotation.select('sample_token','instance_token','visibility_token','attribute_tokens')
    df_sample_annotation.printSchema()
    df_instance = df_instance.select('token','category_token','nbr_annotations')
    df_instance.printSchema()

    # Store data
    df_log.repartition(2000).write.partitionBy("vehicle").mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/sources/log/")
    df_scene.repartition(2000).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/sources/scene/")
    df_sample.repartition(2000).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/sources/sample/")
    df_sample_data.repartition(2000).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/sources/sample_data/")
    df_sample_annotation.repartition(2000).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/sources/sample_annotation/")
    df_instance.repartition(2000).write.mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/silver/sources/instance/")

    spark.stop()

if __name__ == "__main__":
    load_dotenv('etl_variables.env')

    MASTER = os.getenv('MASTER')
    DATASET_VERSION = os.getenv('DATASET_VERSION')
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    GCP_CONNECTOR_LOCATION = os.getenv('GCP_CONNECTOR_LOCATION')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    etl_seeds(MASTER,DATASET_VERSION,GOOGLE_APPLICATION_CREDENTIALS,GCP_CONNECTOR_LOCATION,BUCKET_NAME)
    etl_sources(MASTER,DATASET_VERSION,GOOGLE_APPLICATION_CREDENTIALS,GCP_CONNECTOR_LOCATION,BUCKET_NAME)