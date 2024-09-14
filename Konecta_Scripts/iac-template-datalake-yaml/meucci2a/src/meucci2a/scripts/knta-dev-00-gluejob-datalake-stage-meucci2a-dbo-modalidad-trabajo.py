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

# Script generated for node source - meucci2a - modalidad - trabajo
sourcemeucci2amodalidadtrabajo_node1 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="sourcemeucci2amodalidadtrabajo_node1",
)

# print("==========modalidadtrabajoSrc==========")
# sourcemeucci2amodalidadtrabajo_node1.printSchema()
# sourcemeucci2amodalidadtrabajo_node1.toDF().show(100)
# print("==========================")

# Script generated for node ApplyMapping - meucci2a - modalidad - trabajo
ApplyMappingmeucci2amodalidadtrabajo_node2 = ApplyMapping.apply(
    frame=sourcemeucci2amodalidadtrabajo_node1,
    mappings=[  ( "id_mod_trab", "string", "id_mod_trab", "int"),
                ( "modalidad", "string", "modalidad", "string"),
                ( "anulado", "string", "anulado", "int"),],
    transformation_ctx="ApplyMappingmeucci2amodalidadtrabajo_node2",
)

# print("==========modalidadtrabajoSrc==========")
# ApplyMappingmeucci2amodalidadtrabajo_node2.printSchema()
# ApplyMappingmeucci2amodalidadtrabajo_node2.toDF().show(100)
# print("==========================")

# Script generated for node target - meucci2a - modalidad - trabajo
targetmeucci2amodalidadtrabajo_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMappingmeucci2amodalidadtrabajo_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="targetmeucci2amodalidadtrabajo_node3",
)

BUCKET_NAME = 'knta-dev-00-datalake-stage'
PREFIX = 'meucci2a/knta_dev_00_gluetable_datalake_stage_meucci2a_dbo_modalidad_trabajo'

NEW_FILE_NAME = '/modalidad_trabajo_snappy.parquet'

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