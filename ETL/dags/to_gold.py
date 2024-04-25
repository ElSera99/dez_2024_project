import os

from dotenv import load_dotenv

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

def etl_samples(master,dataset_version,google_application_credentials,gcp_connector_location,bucket_name):
   # Set Spark Session
    conf = SparkConf() \
    .setMaster(master) \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.driver.memory", "12g") \
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

    # Read files
    df_sample_data = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/sources/sample_data/*')
    df_sample = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/sources/sample/*')
    df_scene = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/sources/scene/*')
    df_log = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/sources/log/*')
    
    # Transform Samples
    df_samples_join_1 = df_sample_data.filter(df_sample_data.is_key_frame == True).join(df_sample, df_sample_data.sample_token == df_sample.token, 'inner').drop('token')
    df_samples_join_2 = df_samples_join_1.join(df_scene, df_samples_join_1.scene_token == df_scene.token, 'inner').drop('token')
    df_samples_join_3 = df_samples_join_2.join(df_log, df_samples_join_2.log_token == df_log.token, 'inner').drop('token')
    samples = df_samples_join_3.select('sample_token','logfile','vehicle', 'location', 'scene_name', 'description', 'timestamp_sample_data','ego_pose_token')

    # Write samples
    samples.repartition(500).write.partitionBy("vehicle").mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/gold/samples/")

    spark.stop()


def etl_objects(master,dataset_version,google_application_credentials,gcp_connector_location,bucket_name):
   # Set Spark Session
    conf = SparkConf() \
    .setMaster(master) \
    .setAppName('test') \
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .set("spark.driver.memory", "12g") \
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

    # Read files
    df_category = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/seeds/category/*')
    df_visibility = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/seeds/visibility/*')
    df_sample_annotation = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/sources/sample_annotation/*')
    df_instance = spark.read.parquet(f'gs://{bucket_name}/{dataset_version}/silver/sources/instance/*')

    # Transform data
    df_objects_join_1 = df_sample_annotation.join(df_instance, df_sample_annotation.instance_token == df_instance.token,'inner').drop('token', 'instance_token')
    df_objects_join_2 = df_objects_join_1.join(df_category, df_objects_join_1.category_token == df_category.uid, 'inner') \
                    .withColumnRenamed('description', 'category_description') \
                    .drop('uid', 'category_token')
    objects = df_objects_join_2.join(df_visibility, df_objects_join_2.visibility_token == df_visibility.uid, 'inner') \
                    .drop('uid', 'visibility_token', 'description', 'attribute_tokens')
    
    # Save data
    objects.write.partitionBy("object").mode('overwrite').parquet(f"gs://{bucket_name}/{dataset_version}/gold/objects/")
    
    spark.stop()

if __name__ == "__main__":
    load_dotenv('etl_variables.env')

    MASTER = os.getenv('MASTER')
    DATASET_VERSION = os.getenv('DATASET_VERSION')
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    GCP_CONNECTOR_LOCATION = os.getenv('GCP_CONNECTOR_LOCATION')
    BUCKET_NAME = os.getenv('BUCKET_NAME')

    etl_samples(MASTER,DATASET_VERSION,GOOGLE_APPLICATION_CREDENTIALS,GCP_CONNECTOR_LOCATION,BUCKET_NAME)
    etl_objects(MASTER,DATASET_VERSION,GOOGLE_APPLICATION_CREDENTIALS,GCP_CONNECTOR_LOCATION,BUCKET_NAME)
