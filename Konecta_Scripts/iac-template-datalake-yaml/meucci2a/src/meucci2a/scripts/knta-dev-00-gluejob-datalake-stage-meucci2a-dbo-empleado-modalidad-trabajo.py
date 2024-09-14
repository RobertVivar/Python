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

# Script generated for node source - meucci2a - empleado - modalidad - trabajo
sourcemeucci2aempleadomodalidadtrabajo_node1 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="sourcemeucci2aempleadomodalidadtrabajo_node1",
)

# print("==========empleadomodalidadtrabajoSrc==========")
# sourcemeucci2aempleadomodalidadtrabajo_node1.printSchema()
# sourcemeucci2aempleadomodalidadtrabajo_node1.toDF().show(100)
# print("==========================")

# Script generated for node ApplyMapping - meucci2a - empleado - modalidad - trabajo
ApplyMappingmeucci2aempleadomodalidadtrabajo_node2 = ApplyMapping.apply(
    frame=sourcemeucci2aempleadomodalidadtrabajo_node1,
    mappings=[  ( "id_emp_modtrab", "string", "id_emp_modtrab", "int"),
                ( "emp_id", "string", "emp_id", "int"),
                ( "id_mod_trab", "string", "id_mod_trab", "int"),
                ( "fechadesde", "string", "fechadesde", "timestamp"),
                ( "fechahasta", "string", "fechahasta", "timestamp"),
                ( "anulado", "string", "anulado", "int"),
                ( "fechor", "string", "fechor", "timestamp"),
                ( "emp_id_gen", "string", "emp_id_gen", "int"),
                ( "emp_id_fin", "string", "emp_id_fin", "int"),
                ( "fechor_fin", "string", "fechor_fin", "timestamp"),],
    transformation_ctx="ApplyMappingmeucci2aempleadomodalidadtrabajo_node2",
)

# print("==========empleadomodalidadtrabajoSrc==========")
# ApplyMappingmeucci2aempleadomodalidadtrabajo_node2.printSchema()
# ApplyMappingmeucci2aempleadomodalidadtrabajo_node2.toDF().show(100)
# print("==========================")

# Script generated for node target - meucci2a - empleado - modalidad - trabajo
targetmeucci2aempleadomodalidadtrabajo_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMappingmeucci2aempleadomodalidadtrabajo_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="targetmeucci2aempleadomodalidadtrabajo_node3",
)

BUCKET_NAME = 'knta-dev-00-datalake-stage'
PREFIX = 'meucci2a/knta_dev_00_gluetable_datalake_stage_meucci2a_dbo_empleado_modalidad_trabajo'

NEW_FILE_NAME = '/empleado_modalidad_trabajo_snappy.parquet'

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