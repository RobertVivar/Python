import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME","SOURCE_BUCKET","TARGET_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_client = boto3.client('s3', region_name='us-east-1')

SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]

# Script generated for node source - meucci2a - cargo
sourcemeucci2acargo_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "|", # ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            SOURCE_BUCKET
        ],
        "recurse": True,
    },
    transformation_ctx="sourcemeucci2acargo_node1",
)

# print("==========CargoSrc==========")
# sourcemeucci2acargo_node1.printSchema()
# sourcemeucci2acargo_node1.toDF().show(10)
# print("==========================")

# Script generated for node ApplyMapping - meucci2a - cargo
ApplyMappingmeucci2acargo_node2 = ApplyMapping.apply(
    frame=sourcemeucci2acargo_node1,
    mappings=[  ( "car_id", "string", "car_id", "int"),
                ( "car_nombre", "string", "car_nombre", "string"),
                ( "car_nombrecorto", "string", "car_nombrecorto", "string"),
                ( "anulado", "string", "anulado", "int"),
                ( "car_estmk", "string", "car_estmk", "int"),
                ( "car_essupervisor", "string", "car_essupervisor", "int"),
                ( "car_esjefe", "string", "car_esjefe", "int"),
                ( "car_esgerente", "string", "car_esgerente", "int"),
                ( "car_asignacuenta", "string", "car_asignacuenta", "int"),
                ( "car_horariofijo", "string", "car_horariofijo", "int"),
                ( "car_tienegente", "string", "car_tienegente", "int"),
                ( "car_tipo_id", "string", "car_tipo_id", "int"),
                ( "car_esformadorojt", "string", "car_esformadorojt", "int"),
                ( "car_confidencial", "string", "car_confidencial", "int"),
                ( "car_sociedad", "string", "car_sociedad", "string"),],
    transformation_ctx="ApplyMappingmeucci2acargo_node2",
)

# print("==========CargoSrc==========")
# ApplyMappingmeucci2acargo_node2.printSchema()
# ApplyMappingmeucci2acargo_node2.toDF().show(10)
# print("==========================")

# Script generated for node target - meucci2a - cargo
targetmeucci2acargo_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMappingmeucci2acargo_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="targetmeucci2acargo_node3",
)

BUCKET_NAME = 'knta-dev-00-datalake-stage'
PREFIX = 'meucci2a/knta_dev_00_gluetable_datalake_stage_meucci2a_dbo_cargo'

NEW_FILE_NAME = '/cargo_snappy.parquet'

response = s3_client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX
    )

for clave in response["Contents"]:
  path = clave["Key"]
  word_key = 'run-'
  if word_key in path:
      file_name = path
      
copy_source = {'Bucket' : BUCKET_NAME, 'Key': file_name}

s3_client.copy_object(Bucket=BUCKET_NAME, CopySource=copy_source, Key=PREFIX + NEW_FILE_NAME)
s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_name)

job.commit()