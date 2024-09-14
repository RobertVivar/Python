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

# Script generated for node source - meucci2a - empleado_cuentalucent
sourcemeucci2aempleado_cuentalucent_node1 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="sourcemeucci2aempleado_cuentalucent_node1",
)

# print("==========empleado_cuentalucentSrc==========")
# sourcemeucci2aempleado_cuentalucent_node1.printSchema()
# sourcemeucci2aempleado_cuentalucent_node1.toDF().show(100)
# print("==========================")

# Script generated for node ApplyMapping - meucci2a - empleado_cuentalucent
ApplyMappingmeucci2aempleado_cuentalucent_node2 = ApplyMapping.apply(
    frame=sourcemeucci2aempleado_cuentalucent_node1,
    mappings=[  ( "luc_agente", "string", "luc_agente", "int"),
                ( "luc_fechadesde", "string", "luc_fechadesde", "timestamp"),
                ( "luc_fechahasta", "string", "luc_fechahasta", "timestamp"),
                ( "emp_id", "string", "emp_id", "int"),
                ( "anulado", "string", "anulado", "byte"),
                ( "ori_codigo", "string", "ori_codigo", "int"),
                ( "luc_id", "string", "luc_id", "int"),
                ( "luc_clave", "string", "luc_clave", "string"),],
    transformation_ctx="ApplyMappingmeucci2aempleado_cuentalucent_node2",
)

# print("==========empleado_cuentalucentSrc==========")
# ApplyMappingmeucci2aempleado_cuentalucent_node2.printSchema()
# ApplyMappingmeucci2aempleado_cuentalucent_node2.toDF().show(100)
# print("==========================")

# Script generated for node target - meucci2a - empleado_cuentalucent
targetmeucci2aempleado_cuentalucent_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMappingmeucci2aempleado_cuentalucent_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="targetmeucci2aempleado_cuentalucent_node3",
)

BUCKET_NAME = 'knta-dev-00-datalake-stage'
PREFIX = 'meucci2a/knta_dev_00_gluetable_datalake_stage_meucci2a_dbo_empleado_cuentalucent'

NEW_FILE_NAME = '/empleado_cuentalucent_snappy.parquet'

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