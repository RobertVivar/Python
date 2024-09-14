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

# Script generated for node source - meucci2a - cuenta
sourcemeucci2acuenta_node1 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="sourcemeucci2acuenta_node1",
)

# print("==========cuentaSrc==========")
# sourcemeucci2acuenta_node1.printSchema()
# sourcemeucci2acuenta_node1.toDF().show(100)
# print("==========================")

# Script generated for node ApplyMapping - meucci2a - cuenta
ApplyMappingmeucci2acuenta_node2 = ApplyMapping.apply(
    frame=sourcemeucci2acuenta_node1,
    mappings=[  ( "cue_id", "string", "cue_id", "int"),
                ( "cue_nombre", "string", "cue_nombre", "string"),
                ( "cue_nombrecorto", "string", "cue_nombrecorto", "string"),
                ( "cue_fechaalta", "string", "cue_fechaalta", "timestamp"),
                ( "cue_fechabaja", "string", "cue_fechabaja", "timestamp"),
                ( "cue_esmovicom", "string", "cue_esmovicom", "int"),
                ( "rel_id", "string", "rel_id", "int"),
                ( "cue_calculastaff", "string", "cue_calculastaff", "int"),
                ( "cue_esactionline", "string", "cue_esactionline", "int"),
                ( "cue_mediahora", "string", "cue_mediahora", "string"),
                ( "anulado", "string", "anulado", "int"),
                ( "cue_planificahoras", "string", "cue_planificahoras", "int"),
                ( "habilitada", "string", "habilitada", "int"),
                ( "pai_id", "string", "pai_id", "int"),
                ( "cue_tipo", "string", "cue_tipo", "string"),],
    transformation_ctx="ApplyMappingmeucci2acuenta_node2",
)

# print("==========cuentaSrc==========")
# ApplyMappingmeucci2acuenta_node2.printSchema()
# ApplyMappingmeucci2acuenta_node2.toDF().show(100)
# print("==========================")

# Script generated for node target - meucci2a - cuenta
targetmeucci2acuenta_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMappingmeucci2acuenta_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="targetmeucci2acuenta_node3",
)

BUCKET_NAME = 'knta-dev-00-datalake-stage'
PREFIX = 'meucci2a/knta_dev_00_gluetable_datalake_stage_meucci2a_dbo_cuenta'

NEW_FILE_NAME = '/cuenta_snappy.parquet'

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