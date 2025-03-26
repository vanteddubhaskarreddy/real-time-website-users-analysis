import ast
import sys
import signal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json, udf
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
print(args)
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
    StructField("referrer", StringType(), True),
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


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {output_table} (
  url STRING,
  referrer STRING,
  user_agent STRUCT<
    family: STRING,
    major: STRING,
    minor: STRING,
    patch: STRING,
    device: STRUCT<
      family: STRING,
      major: STRING,
      minor: STRING,
      patch: STRING
    >,
    os: STRUCT<
      family: STRING,
      major: STRING,
      minor: STRING,
      patch: STRING
    >
  >,
  headers MAP<STRING, STRING>,
  host STRING,
  ip STRING,
  event_time TIMESTAMP,
  instructor_name STRING,
  follower_count BIGINT,
  offers_bootcamp BOOLEAN
)
USING ICEBERG
PARTITIONED BY (hours(event_time))
""")

# Read from Kafka in batch mode
kafka_df = (spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()
)

def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

reference_df = spark.sql("SELECT * FROM zachwilson.host_reference_table")
last_refresh = datetime.now()

def insert_new_kafka_data(df, batch_id):
    global reference_df
    global last_refresh
    now = datetime.now()
    td = now - last_refresh
    td_mins = int(round(td.total_seconds() / 60))

    # Only refresh the reference data every 30 minutes
    if td_mins >= 30:
        reference_df = spark.sql("SELECT * FROM zachwilson.host_reference_table")
        last_refresh = datetime.now()
    # Extract the value from Kafka messages and cast to String
    new_df = df.select("key", "value", "topic") \
        .withColumn("decoded_value", decode_udf(col("value"))) \
        .withColumn("value", from_json(col("decoded_value"), schema)) \
        .select("value.*") \
        .join(reference_df, reference_df["hostname"] == col("host"), "left") \
        .select("url",
                "referrer",
                "user_agent",
                "headers",
                "host",
                "ip",
                "event_time",
                "instructor_name",
                "follower_count",
                "offers_bootcamp"
                )
    view_name = "kafka_source_" + str(batch_id)
    new_df.createOrReplaceGlobalTempView(view_name)
    spark.sql(f"""
    MERGE INTO {output_table} AS t
        USING global_temp.{view_name} AS s
         ON t.ip = s.ip AND t.event_time = s.event_time
        WHEN NOT MATCHED THEN INSERT *
    """)



query = kafka_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(insert_new_kafka_data) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# stop the job after 5 minutes
# PLEASE DO NOT REMOVE TIMEOUT
query.awaitTermination(timeout=60*5)


