import ast
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, MapType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

spark = (SparkSession.builder
         .getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table', 'kafka_credentials'])
run_date = args['ds']
output_table = args['output_table']
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
end_timestamp = f"{run_date}T23:59:59.999Z"

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
    StructField("host", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

# Read from Kafka in batch mode
kafka_df = (spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";') \
    .load()
)

def decode_col(column):
    return column.decode('utf-8')

decode_udf = udf(decode_col, StringType())

# Extract the value from Kafka messages and cast to String
kafka_df = kafka_df.select("key", "value", "topic") \
        .withColumn("decoded_value", decode_udf(col("value"))) \
        .withColumn("value", from_json(col("decoded_value"), schema))\
        .select("value.*").filter(
    (col("event_time") >= start_timestamp) &
    (col("event_time") <= end_timestamp)
)

# Add these lines before writing to table
print(f"Total records in Kafka: {kafka_df.count()}")
kafka_df.show(5, truncate=False)  # Show 5 sample records

# Check if table exists
table_exists = spark._jsparkSession.catalog().tableExists(output_table)

if table_exists:
    # If table exists, use overwritePartitions
    (kafka_df
        .writeTo(output_table)
        .tableProperty("write.spark.fanout.enabled", "true")
        .overwritePartitions()
    )
else:
    # If table doesn't exist, create it
    (kafka_df
        .writeTo(output_table)
        .tableProperty("write.spark.fanout.enabled", "true")
        .createOrReplace()
    )

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
