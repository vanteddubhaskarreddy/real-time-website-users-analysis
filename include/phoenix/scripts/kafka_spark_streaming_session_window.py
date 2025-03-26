import ast
import sys
import requests
import json
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json, udf, session_window, collect_set, min, max, first, count, when, sha2, concat, hex, abs, xxhash64
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime



spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME",
                                     "ds",
                                     'output_table',
                                     'kafka_credentials',
                                     'checkpoint_location'
                                     ])
run_date = args['ds']
output_table = args['output_table']
checkpoint_location = args['checkpoint_location']
kafka_credentials = ast.literal_eval(args['kafka_credentials'])
glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

# Retrieve Kafka credentials from environment variables
kafka_key = kafka_credentials['KAFKA_WEB_TRAFFIC_KEY']
kafka_secret = kafka_credentials['KAFKA_WEB_TRAFFIC_SECRET']
kafka_bootstrap_servers = kafka_credentials['KAFKA_WEB_BOOTSTRAP_SERVER']
kafka_topic = kafka_credentials['KAFKA_TOPIC']

if kafka_key is None or kafka_secret is None:
    raise ValueError("KAFKA_WEB_TRAFFIC_KEY and KAFKA_WEB_TRAFFIC_SECRET must be set as environment variables.")

# Kafka configuration

start_timestamp = f"{run_date}T00:00:00.000Z"

# Define the schema of the Kafka message value
schema = StructType([
    StructField("url", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("academy_id", IntegerType(), True),
    StructField("user_agent", StructType([
        StructField("family", StringType(), True),
        StructField("major", StringType(), True),
        StructField("minor", StringType(), True),
        StructField("patch", StringType(), True),
        StructField("device", StructType([
            StructField("family", StringType(), True),
            StructField("major", StringType(), True),
            StructField("minor", StringType(), True),
            StructField("patch", StringType(), True),
        ]), True),
        StructField("os", StructType([
            StructField("family", StringType(), True),
            StructField("major", StringType(), True),
            StructField("minor", StringType(), True),
            StructField("patch", StringType(), True),
        ]), True)
    ]), True),
    StructField("headers", MapType(keyType=StringType(), valueType=StringType()), True),
    StructField("host", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("event_time", TimestampType(), True)
])



# Create a table to store the session window data
spark.sql(f""" 
CREATE TABLE IF NOT EXISTS {output_table} (
session_id BIGINT,
user_id BIGINT,
window_start TIMESTAMP,
window_end TIMESTAMP,
total_events BIGINT,
country STRING,
state STRING,
city STRING,
os STRING,
device STRING,
is_user BOOLEAN
)
USING iceberg
""")

geocache = {}
GEOCODING_API_KEY = '403640E71A8CD7BAD8EA3BB7750822D7'

def geocode_ip_address(ip):
    global geocache

    if ip in geocache:
        return geocache[ip]
    
    url = "https://api.ip2location.io"
    response = requests.get(url, params = {'ip':ip, 'key':GEOCODING_API_KEY})

    if response.status_code != 200:
        # Log the error
        print(f"Error: {response.text}")
        return {}
    

    data = json.loads(response.text)

    country = data.get('country_code', '')
    state = data.get('region_name', '')
    city = data.get('city_name', '')

    geocache[ip] = {'country': country, 'state': state, 'city': city}
    return geocache[ip]

kafka_df = (spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetPerTrigger", 10000) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()
    )

def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

geocode_schema = StructType([
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
])

geocode_udf = udf(geocode_ip_address, geocode_schema)

parsed_df = kafka_df \
    .withColumn("decoded_value", decode_udf(col("value"))) \
    .withColumn("value", from_json(col("decoded_value"), schema)) \
    .withColumn("geodata", geocode_udf(col("value.ip"))) \
    .withWatermark("timestamp", "30 seconds")

session_window_df = parsed_df \
    .groupBy(session_window(col("timestamp"), "5 minutes"),
             col("value.user_id")
             ) \
    .agg(
        count("*").alias("total_events"),
        first("geodata.country").alias("country"),
        first("geodata.state").alias("state"),
        first("geodata.city").alias("city"),
        first("value.user_agent.os.family").alias("os"),
        first("value.user_agent.device.family").alias("device"),
        when(col("value.user_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("is_user")
    ).select(
        col("user_id"),
        col("session_window.start").alias("window_start"),
        col("session_window.end").alias("window_end"),
        col("total_events"),
        col("country"),
        col("state"),
        col("city"),
        col("os"),
        col("device"),
        col("is_user")
    )

formatted_sessions = session_window_df.select(
    abs(xxhash64(concat(
        col("user_id").cast("string"),
        col("window_start").cast("string"),
        col("window_end").cast("string")
    ))).cast("bigint").alias("session_id"),
    col("user_id").cast("bigint"),
    col("window_start"),
    col("window_end"),
    col("total_events").cast("bigint"),
    col("country"),
    col("state"),
    col("city"),
    col("os"),
    col("device"),
    col("is_user")
)

# Write to the output table
query = formatted_sessions \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("fanout-enabled", "true") \
    .option("checkpointLocation", checkpoint_location) \
    .toTable(output_table)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*30)

