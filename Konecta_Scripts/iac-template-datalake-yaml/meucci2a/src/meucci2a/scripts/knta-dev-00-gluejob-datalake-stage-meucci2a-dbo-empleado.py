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

# Script generated for node source - meucci2a - empleado
sourcemeucci2aempleado_node1 = glueContext.create_dynamic_frame.from_options(
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
    transformation_ctx="sourcemeucci2aempleado_node1",
)

# print("==========empleadoSrc==========")
# sourcemeucci2aempleado_node1.printSchema()
# sourcemeucci2aempleado_node1.toDF().show(100)
# print("==========================")

# Script generated for node ApplyMapping - meucci2a - empleado
ApplyMappingmeucci2aempleado_node2 = ApplyMapping.apply(
    frame=sourcemeucci2aempleado_node1,
    mappings=[  ( "emp_id", "string", "emp_id", "int"),
                ( "emp_legbejerman", "string", "emp_legbejerman", "int"),
                ( "emp_apellido", "string", "emp_apellido", "string"),
                ( "emp_nombre", "string", "emp_nombre", "string"),
                ( "emp_doctipo", "string", "emp_doctipo", "string"),
                ( "emp_docnro", "string", "emp_docnro", "int"),
                ( "emp_fechanac", "string", "emp_fechanac", "timestamp"),
                ( "emp_cuil", "string", "emp_cuil", "string"),
                ( "emp_sexo", "string", "emp_sexo", "string"),
                ( "eci_id", "string", "eci_id", "int"),
                ( "emp_calle", "string", "emp_calle", "string"),
                ( "emp_nro", "string", "emp_nro", "string"),
                ( "emp_piso", "string", "emp_piso", "string"),
                ( "emp_dpto", "string", "emp_dpto", "string"),
                ( "bar_id", "string", "bar_id", "int"),
                ( "loc_id", "string", "loc_id", "int"),
                ( "prov_id", "string", "prov_id", "int"),
                ( "pai_id", "string", "pai_id", "int"),
                ( "emp_telparticular", "string", "emp_telparticular", "string"),
                ( "emp_telcelular", "string", "emp_telcelular", "string"),
                ( "emp_email_externo", "string", "emp_email_externo", "string"),
                ( "emp_email", "string", "emp_email", "string"),
                ( "emp_cp", "string", "emp_cp", "string"),
                ( "emp_alergias", "string", "emp_alergias", "string"),
                ( "emp_gruposanguineo", "string", "emp_gruposanguineo", "string"),
                ( "oso_id", "string", "oso_id", "int"),
                ( "oso_tipocobertura", "string", "oso_tipocobertura", "string"),
                ( "emp_archivofoto", "string", "emp_archivofoto", "string"),
                ( "anulado", "string", "anulado", "int"),
                ( "bloqueado", "string", "bloqueado", "int"),
                ( "quienlobloqueo", "string", "quienlobloqueo", "string"),
                ( "emp_pass", "string", "emp_pass", "string"),
                ( "emp_telinterno", "string", "emp_telinterno", "string"),
                ( "emp_telcelularpublico", "string", "emp_telcelularpublico", "string"),
                ( "ubi_id", "string", "ubi_id", "int"),
                ( "emp_observacionesubicacion", "string", "emp_observacionesubicacion", "string"),
                ( "emp_licenciaprolongada", "string", "emp_licenciaprolongada", "int"),
                ( "suc_id", "string", "suc_id", "int"),
                ( "loc_id_nac", "string", "loc_id_nac", "int"),
                ( "Pro_id_nac", "string", "Pro_id_nac", "int"),
                ( "pais_id_nac", "string", "pais_id_nac", "int"),
                ( "suc_id_cobra", "string", "suc_id_cobra", "int"),
                ( "curr_id", "string", "curr_id", "int"),
                ( "gsa_id", "string", "gsa_id", "int"),
                ( "emp_ultimaactualizacioninteraction", "string", "emp_ultimaactualizacioninteraction", "timestamp"),
                ( "emp_foto", "string", "emp_foto", "string"),
                ( "emp_nuevomail", "string", "emp_nuevomail", "string"),
                ( "tipodoc_id", "string", "tipodoc_id", "int"),
                ( "aus_id", "string", "aus_id", "int"),
                ( "sociedad_id", "string", "sociedad_id", "int"),
                ( "idpersonalsap", "string", "idpersonalsap", "int"),
                ( "idm_id", "string", "idm_id", "int"),
                ( "sex_id", "string", "sex_id", "int"),
                ( "emp_fechacasam", "string", "emp_fechacasam", "timestamp"),
                ( "emp_canthijos", "string", "emp_canthijos", "int"),
                ( "emp_altatemprana", "string", "emp_altatemprana", "boolean"),
                ( "emp_fechalta", "string", "emp_fechalta", "timestamp"),
                ( "emp_fechfinaltatemp", "string", "emp_fechfinaltatemp", "timestamp"),
                ( "transferir", "string", "transferir", "boolean"),
                ( "cnt_fijo", "string", "cnt_fijo", "int"),],
    transformation_ctx="ApplyMappingmeucci2aempleado_node2",
)

# print("==========empleadoSrc==========")
# ApplyMappingmeucci2aempleado_node2.printSchema()
# ApplyMappingmeucci2aempleado_node2.toDF().show(100)
# print("==========================")

# Script generated for node target - meucci2a - empleado
targetmeucci2aempleado_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMappingmeucci2aempleado_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="targetmeucci2aempleado_node3",
)

BUCKET_NAME = 'knta-dev-00-datalake-stage'
PREFIX = 'meucci2a/knta_dev_00_gluetable_datalake_stage_meucci2a_dbo_empleado'

NEW_FILE_NAME = '/empleado_snappy.parquet'

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