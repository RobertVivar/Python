import sys

import pytz
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, date, timedelta
from botocore.exceptions import ClientError
# NO DEFAULT IMPORTS
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'AWS_REGION',
    'SOURCE_DATABASE',
    'SOURCE_TABLE_CALENDARIO',
    'SOURCE_TABLE_EMPLEADO_CUENTA_LUCENT',
    'SOURCE_TABLE_EMPLEADO_SISTEMA_EXTERNO',
    'SOURCE_TABLE_SISTEMA_EXTERNO',
    'TARGET_BUCKET',
    'TARGET_DATABASE',
    'TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS',
    'PARAM_INT_DAYS',
])

print("args: ")
print(args)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# VARIABLES

TIMEZONE = pytz.timezone('America/Lima')
AWS_REGION = args['AWS_REGION']
print(f"in region {AWS_REGION}")
now = str(datetime.now(TIMEZONE))
TARGET_DAYS = int(args["PARAM_INT_DAYS"])  # 30
FECHA_INI = datetime.combine(datetime.now(TIMEZONE).date() - timedelta(days=TARGET_DAYS), datetime.min.time())
FECHA_FIN = datetime.combine(datetime.now(TIMEZONE).date(), datetime.min.time())
print(f"FECHA_INI:{FECHA_INI} - FECHA_FIN:{FECHA_FIN}")
# "knta-0002-ml-mld-gluedb-00-datalake-raw"
SOURCE_DATABASE = args["SOURCE_DATABASE"]
# "knta_0002_ml_mld_gluetable_00_datalake_raw_meucci2a_meucci2a_dbo_calendario",
SOURCE_TABLE_CALENDARIO = args["SOURCE_TABLE_CALENDARIO"]
# "knta_0002_ml_mld_gluetable_00_datalake_raw_meucci2a_meucci2a_dbo_empleado_cuentalucent"
SOURCE_TABLE_EMPLEADO_CUENTA_LUCENT = args["SOURCE_TABLE_EMPLEADO_CUENTA_LUCENT"]
# "knta_0002_ml_mld_gluetable_00_datalake_raw_meucci_2ameucci2a_dbo_empleado_sistemaexterno"
SOURCE_TABLE_EMPLEADO_SISTEMA_EXTERNO = args["SOURCE_TABLE_EMPLEADO_SISTEMA_EXTERNO"]
# "knta_0002_ml_mld_gluetable_00_datalake_raw_meucci_2ameucci2a_dbo_sistemaexterno"
SOURCE_TABLE_SISTEMA_EXTERNO = args["SOURCE_TABLE_SISTEMA_EXTERNO"]
# "knta-0002-ml-mld-gluedb-00-datalake-application"
# "knta_0002_ml_mld_gluetable_00_datalake_application_maestro_base_maestro_equipo_usuarios"
TARGET_DATABASE = args["TARGET_DATABASE"]
TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS = args["TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS"]
TARGET_BUCKET = args["TARGET_BUCKET"]

AVAYA = 1
MITROL = 2
VOCALCOM = 4
AVAYAKCRM = 7
LOGINACD = 8
NEOTEL = 9

s3_client = boto3.client('s3', region_name='us-east-1')
glue_client = boto3.client("glue", region_name=AWS_REGION)


# FUNCTIONS
def load_dynamic_frame_to(dyf: DynamicFrame, database: str, table: str, partitions: [str] = None):
    _table = glue_client.get_table(
        DatabaseName=database,
        Name=table,
    )

    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": _table["Table"]["StorageDescriptor"]["Location"],
            "partitionKeys": partitions
        },
        format="parquet")


######### CALENDARIO ###############################################################
CalendarioSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CALENDARIO,
    transformation_ctx="CalendarioSrc",
)
CalendarioSelectFields = SelectFields.apply(
    frame=CalendarioSrc,
    paths=["fecha"],
    transformation_ctx="CalendarioSelectFields",
)
CalendarioSelectFields = Filter.apply(
    frame=CalendarioSelectFields,
    f=lambda x: FECHA_INI <= x["fecha"] <= FECHA_FIN
)
Calendario = ApplyMapping.apply(
    frame=CalendarioSelectFields,
    mappings=[("fecha", "timestamp", "calendario_fecha", "timestamp")],
    transformation_ctx="Calendario_ApplyMapping",
)

####### EMPLEADO_CUENTA_LUCENT ###############################################################
EmpleadoCuentaLucentSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_CUENTA_LUCENT,
    transformation_ctx="EmpleadoCuentaLucentSrc",
)
EmpleadoCuentaLucentFields = SelectFields.apply(
    frame=EmpleadoCuentaLucentSrc,
    paths=["emp_id", "luc_agente", "luc_fechadesde", "luc_fechahasta", "anulado", "ori_codigo"],
    transformation_ctx="EmpleadoCuentaLucentFields",
)
EmpleadoCuentaLucentNoAnulado = Filter.apply(
    frame=EmpleadoCuentaLucentFields,
    f=lambda x: x["anulado"] == 0
)
EmpleadoCuentaLucent = ApplyMapping.apply(
    frame=EmpleadoCuentaLucentNoAnulado,
    mappings=[
        ("emp_id", "int", "empleado_cuenta_lucent_emp_id", "int"),
        ("luc_agente", "int", "empleado_cuenta_lucent_luc_agente", "int"),
        ("luc_fechadesde", "timestamp", "empleado_cuenta_lucent_luc_fechadesde", "timestamp"),
        ("luc_fechahasta", "timestamp", "empleado_cuenta_lucent_luc_fechahasta", "timestamp"),
        ("anulado", "tinyint", "empleado_cuenta_lucent_anulado", "byte"),
        ("ori_codigo", "int", "empleado_cuenta_lucent_ori_codigo", "int"),
    ],
    transformation_ctx="EmpleadoCuentaLucent",
)

####### EMPLEADOCUENTA_LUCENT AVAYA ###############################################################
EmpleadoCuentaLucentAvaya = Filter.apply(
    frame=EmpleadoCuentaLucent,
    f=lambda x: x["empleado_cuenta_lucent_ori_codigo"] == AVAYA
)
cal_df = Calendario.toDF()
emp_cuenta_lucent_avaya_df = EmpleadoCuentaLucentAvaya.toDF()

emp_cuenta_lucent_avaya_df = emp_cuenta_lucent_avaya_df.withColumn(
    'tmp_empleado_cuenta_lucent_luc_fechahasta',
    F.when(
        emp_cuenta_lucent_avaya_df.empleado_cuenta_lucent_luc_fechahasta.isNull(), datetime.now(TIMEZONE)
    ).otherwise(emp_cuenta_lucent_avaya_df.empleado_cuenta_lucent_luc_fechahasta)
)

lista_avaya_join01_df = cal_df.join(
    emp_cuenta_lucent_avaya_df,
    (cal_df.calendario_fecha >= emp_cuenta_lucent_avaya_df.empleado_cuenta_lucent_luc_fechadesde) &
    (cal_df.calendario_fecha <= emp_cuenta_lucent_avaya_df.tmp_empleado_cuenta_lucent_luc_fechahasta)
    , how='inner')

lista_avaya_join01_df = lista_avaya_join01_df.drop("tmp_empleado_cuenta_lucent_luc_fechahasta")

ListaAvayaSrc = DynamicFrame.fromDF(
    lista_avaya_join01_df.select([
        "calendario_fecha",
        "empleado_cuenta_lucent_emp_id",
        "empleado_cuenta_lucent_luc_agente",
        F.row_number().over(
            Window.partitionBy(
                lista_avaya_join01_df['calendario_fecha'],
                lista_avaya_join01_df['empleado_cuenta_lucent_emp_id']
            ).orderBy(lista_avaya_join01_df['empleado_cuenta_lucent_luc_agente'])
        ).alias("fila")
    ]),
    glueContext,
    "ListaAvayaSrc",
)
ListaAvayaFila1 = Filter.apply(
    frame=ListaAvayaSrc,
    f=lambda x: x["fila"] == 1
)
ListaAvaya = ApplyMapping.apply(
    frame=ListaAvayaFila1,
    mappings=[
        ("calendario_fecha", "timestamp", "lista_avaya_fecha", "timestamp"),
        ("empleado_cuenta_lucent_emp_id", "int", "lista_avaya_emp_id", "int"),
        ("empleado_cuenta_lucent_luc_agente", "int", "lista_avaya_id", "int"),
        ("fila", "int", "lista_avaya_fila", "int"),
    ],
    transformation_ctx="ListaAvaya",
)
ListaAvaya = DynamicFrame.fromDF(
    ListaAvaya.toDF().orderBy(["lista_avaya_fecha", "lista_avaya_emp_id"], ascending=[1, 1]),
    glueContext,
    "ListaAvayaSort",
)

####### EMPLEADOCUENTA_LUCENT MITROL ###############################################################
EmpleadoCuentaLucentmitrol = Filter.apply(
    frame=EmpleadoCuentaLucent,
    f=lambda x: x["empleado_cuenta_lucent_ori_codigo"] == MITROL
)
emp_cuenta_lucent_mitrol_df = EmpleadoCuentaLucentmitrol.toDF()
emp_cuenta_lucent_mitrol_df = emp_cuenta_lucent_mitrol_df.withColumn(
    'tmp_empleado_cuenta_lucent_luc_fechahasta',
    F.when(
        emp_cuenta_lucent_mitrol_df.empleado_cuenta_lucent_luc_fechahasta.isNull(),
        datetime.now(TIMEZONE)
    ).otherwise(emp_cuenta_lucent_mitrol_df.empleado_cuenta_lucent_luc_fechahasta)
)

lista_mitrol_join01_df = cal_df.join(
    emp_cuenta_lucent_mitrol_df,
    (cal_df.calendario_fecha >= emp_cuenta_lucent_mitrol_df.empleado_cuenta_lucent_luc_fechadesde) &
    (cal_df.calendario_fecha <= emp_cuenta_lucent_mitrol_df.tmp_empleado_cuenta_lucent_luc_fechahasta)
    , how='inner'
)

lista_mitrol_join01_df = lista_mitrol_join01_df.drop("tmp_empleado_cuenta_lucent_luc_fechahasta")

ListaMitrolSrc = DynamicFrame.fromDF(
    lista_mitrol_join01_df.select([
        "calendario_fecha",
        "empleado_cuenta_lucent_emp_id",
        "empleado_cuenta_lucent_luc_agente",
        F.row_number().over(
            Window.partitionBy(
                lista_mitrol_join01_df['calendario_fecha'],
                lista_mitrol_join01_df['empleado_cuenta_lucent_emp_id']
            ).orderBy(lista_mitrol_join01_df['empleado_cuenta_lucent_luc_agente'])
        ).alias("fila")
    ]),
    glueContext,
    "ListaMitrolSrc",
)
ListaMitrolFila1 = Filter.apply(
    frame=ListaMitrolSrc,
    f=lambda x: x["fila"] == 1
)
ListaMitrol = ApplyMapping.apply(
    frame=ListaMitrolFila1,
    mappings=[
        ("calendario_fecha", "timestamp", "lista_mitrol_fecha", "timestamp"),
        ("empleado_cuenta_lucent_emp_id", "int", "lista_mitrol_emp_id", "int"),
        ("empleado_cuenta_lucent_luc_agente", "int", "lista_mitrol_id", "int"),
        ("fila", "int", "lista_mitrol_fila", "int"),
    ],
    transformation_ctx="ListaMitrol",
)
ListaMitrol = DynamicFrame.fromDF(
    ListaMitrol.toDF().orderBy(["lista_mitrol_fecha", "lista_mitrol_emp_id"], ascending=[1, 1]),
    glueContext,
    "ListaMitrolSort",
)

####### MERGE CALENDARIO AVAYA VS MITROL #######
df_avaya = ListaAvaya.toDF()
df_mitrol = ListaMitrol.toDF()

df_merge_lvl1 = df_avaya.join(
    df_mitrol,
    (df_avaya.lista_avaya_fecha == df_mitrol.lista_mitrol_fecha) &
    (df_avaya.lista_avaya_emp_id == df_mitrol.lista_mitrol_emp_id)
    , how='outer'
).select(
    df_avaya.lista_avaya_fecha, df_avaya.lista_avaya_emp_id, df_avaya.lista_avaya_id,
    df_mitrol.lista_mitrol_id, df_mitrol.lista_mitrol_fecha, df_mitrol.lista_mitrol_emp_id
)

df_derived_column_lvl1 = df_merge_lvl1.withColumn(
    'tmp_lista_fecha',
    F.when(
        df_merge_lvl1.lista_avaya_fecha.isNull(),
        df_merge_lvl1.lista_mitrol_fecha
    ).otherwise(df_merge_lvl1.lista_avaya_fecha)
).drop(df_merge_lvl1.lista_avaya_fecha)

df_derived_column_lvl2 = df_derived_column_lvl1.withColumn(
    'tmp_lista_emp_id',
    F.when(
        df_derived_column_lvl1.lista_avaya_emp_id.isNull(),
        df_derived_column_lvl1.lista_mitrol_emp_id
    ).otherwise(df_derived_column_lvl1.lista_avaya_emp_id)
).drop(df_derived_column_lvl1.lista_avaya_emp_id)

df_datajoin01 = df_derived_column_lvl2.select(df_derived_column_lvl2.tmp_lista_fecha.alias('lista_fecha'),
                                              df_derived_column_lvl2.tmp_lista_emp_id.alias('lista_emp_id'),
                                              df_derived_column_lvl2.lista_avaya_id,
                                              df_derived_column_lvl2.lista_mitrol_id)

####### EMPLEADOCUENTA_LUCENT VOCALCOM ###############################################################
EmpleadoCuentaLucentvocalcom = Filter.apply(
    frame=EmpleadoCuentaLucent,
    f=lambda x: x["empleado_cuenta_lucent_ori_codigo"] == VOCALCOM
)
emp_cuenta_lucent_vocalcom_df = EmpleadoCuentaLucentvocalcom.toDF()
emp_cuenta_lucent_vocalcom_df = emp_cuenta_lucent_vocalcom_df.withColumn(
    'tmp_empleado_cuenta_lucent_luc_fechahasta',
    F.when(
        emp_cuenta_lucent_vocalcom_df.empleado_cuenta_lucent_luc_fechahasta.isNull(),
        datetime.now(TIMEZONE)
    ).otherwise(emp_cuenta_lucent_vocalcom_df.empleado_cuenta_lucent_luc_fechahasta)
)

lista_vocalcom_join01_df = cal_df.join(
    emp_cuenta_lucent_vocalcom_df,
    (cal_df.calendario_fecha >= emp_cuenta_lucent_vocalcom_df.empleado_cuenta_lucent_luc_fechadesde) &
    (cal_df.calendario_fecha <= emp_cuenta_lucent_vocalcom_df.tmp_empleado_cuenta_lucent_luc_fechahasta)
    , how='inner'
)

lista_vocalcom_join01_df = lista_vocalcom_join01_df.drop("tmp_empleado_cuenta_lucent_luc_fechahasta")

ListaVocalcomSrc = DynamicFrame.fromDF(
    lista_vocalcom_join01_df.select([
        "calendario_fecha",
        "empleado_cuenta_lucent_emp_id",
        "empleado_cuenta_lucent_luc_agente",
        F.row_number().over(
            Window.partitionBy(
                lista_vocalcom_join01_df['calendario_fecha'],
                lista_vocalcom_join01_df['empleado_cuenta_lucent_emp_id']
            ).orderBy(lista_vocalcom_join01_df['empleado_cuenta_lucent_luc_agente'])
        ).alias("fila")
    ]),
    glueContext,
    "ListaVocalcomSrc",
)
ListaVocalcomFila1 = Filter.apply(
    frame=ListaVocalcomSrc,
    f=lambda x: x["fila"] == 1
)
ListaVocalcom = ApplyMapping.apply(
    frame=ListaVocalcomFila1,
    mappings=[
        ("calendario_fecha", "timestamp", "lista_vocalcom_fecha", "timestamp"),
        ("empleado_cuenta_lucent_emp_id", "int", "lista_vocalcom_emp_id", "int"),
        ("empleado_cuenta_lucent_luc_agente", "int", "lista_vocalcom_id", "int"),
        ("fila", "int", "lista_vocalcom_fila", "int"),
    ],
    transformation_ctx="ListaVocalcom",
)
ListaVocalcom = DynamicFrame.fromDF(
    ListaVocalcom.toDF().orderBy(["lista_vocalcom_fecha", "lista_vocalcom_emp_id"], ascending=[1, 1]),
    glueContext,
    "ListaVocalcomSort",
)

####### MERGE CALENDARIO SET VS VOCALCOM #######
# df_datajoin01
df_vocalcom = ListaVocalcom.toDF()

df_merge_vocalcom = df_datajoin01.join(
    df_vocalcom,
    (df_datajoin01.lista_fecha == df_vocalcom.lista_vocalcom_fecha) &
    (df_datajoin01.lista_emp_id == df_vocalcom.lista_vocalcom_emp_id)
    , how='outer'
).select(
    df_datajoin01.lista_fecha, df_datajoin01.lista_emp_id, df_datajoin01.lista_avaya_id,
    df_datajoin01.lista_mitrol_id,
    df_vocalcom.lista_vocalcom_id, df_vocalcom.lista_vocalcom_fecha,
    df_vocalcom.lista_vocalcom_emp_id
)

df_derived_column_vocalcom_lvl1 = df_merge_vocalcom.withColumn(
    'tmp_lista_fecha',
    F.when(
        df_merge_vocalcom.lista_fecha.isNull(), df_merge_vocalcom.lista_vocalcom_fecha
    ).otherwise(df_merge_vocalcom.lista_fecha)
).drop(df_merge_vocalcom.lista_fecha)

df_derived_column_vocalcom_lvl2 = df_derived_column_vocalcom_lvl1.withColumn(
    'tmp_lista_emp_id',
    F.when(
        df_derived_column_vocalcom_lvl1.lista_emp_id.isNull(),
        df_derived_column_vocalcom_lvl1.lista_vocalcom_emp_id
    ).otherwise(df_derived_column_vocalcom_lvl1.lista_emp_id)
).drop(df_derived_column_vocalcom_lvl1.lista_emp_id)
df_datajoin02 = df_derived_column_vocalcom_lvl2.select(
    df_derived_column_vocalcom_lvl2.tmp_lista_fecha.alias('lista_fecha'),
    df_derived_column_vocalcom_lvl2.tmp_lista_emp_id.alias('lista_emp_id'),
    df_derived_column_vocalcom_lvl2.lista_avaya_id,
    df_derived_column_vocalcom_lvl2.lista_mitrol_id,
    df_derived_column_vocalcom_lvl2.lista_vocalcom_id
)

####### EMPLEADOCUENTA_LUCENT AVAYAKCRM ###############################################################
EmpleadoCuentaLucentavayakcrm = Filter.apply(
    frame=EmpleadoCuentaLucent,
    f=lambda x: x["empleado_cuenta_lucent_ori_codigo"] == AVAYAKCRM
)
emp_cuenta_lucent_avayakcrm_df = EmpleadoCuentaLucentavayakcrm.toDF()
emp_cuenta_lucent_avayakcrm_df = emp_cuenta_lucent_avayakcrm_df.withColumn(
    'tmp_empleado_cuenta_lucent_luc_fechahasta',
    F.when(
        emp_cuenta_lucent_avayakcrm_df.empleado_cuenta_lucent_luc_fechahasta.isNull(),
        datetime.now(TIMEZONE)
    ).otherwise(emp_cuenta_lucent_avayakcrm_df.empleado_cuenta_lucent_luc_fechahasta)
)

lista_avayakcrm_join01_df = cal_df.join(
    emp_cuenta_lucent_avayakcrm_df,
    (cal_df.calendario_fecha >= emp_cuenta_lucent_avayakcrm_df.empleado_cuenta_lucent_luc_fechadesde) &
    (cal_df.calendario_fecha <= emp_cuenta_lucent_avayakcrm_df.tmp_empleado_cuenta_lucent_luc_fechahasta)
    , how='inner')

lista_avayakcrm_join01_df = lista_avayakcrm_join01_df.drop("tmp_empleado_cuenta_lucent_luc_fechahasta")

ListaAvayakcrmSrc = DynamicFrame.fromDF(
    lista_avayakcrm_join01_df.select([
        "calendario_fecha",
        "empleado_cuenta_lucent_emp_id",
        "empleado_cuenta_lucent_luc_agente",
        F.row_number().over(
            Window.partitionBy(
                lista_avayakcrm_join01_df['calendario_fecha'],
                lista_avayakcrm_join01_df['empleado_cuenta_lucent_emp_id']
            ).orderBy(lista_avayakcrm_join01_df['empleado_cuenta_lucent_luc_agente'])
        ).alias("fila")
    ]),
    glueContext,
    "ListaAvayakcrmSrc",
)
ListaAvayakcrmFila1 = Filter.apply(
    frame=ListaAvayakcrmSrc,
    f=lambda x: x["fila"] == 1
)
ListaAvayakcrm = ApplyMapping.apply(
    frame=ListaAvayakcrmFila1,
    mappings=[
        ("calendario_fecha", "timestamp", "lista_avayakcrm_fecha", "timestamp"),
        ("empleado_cuenta_lucent_emp_id", "int", "lista_avayakcrm_emp_id", "int"),
        ("empleado_cuenta_lucent_luc_agente", "int", "lista_avayakcrm_id", "int"),
        ("fila", "int", "lista_avayakcrm_fila", "int"),
    ],
    transformation_ctx="ListaDavox",
)
ListaAvayakcrm = DynamicFrame.fromDF(
    ListaAvayakcrm.toDF().orderBy(["lista_avayakcrm_fecha", "lista_avayakcrm_emp_id"], ascending=[1, 1]),
    glueContext,
    "ListaAvayakcrmSort",
)

####### MERGE CALENDARIO SET VS AVAYAKCRM #######
# df_datajoin02
df_avayakcrm = ListaAvayakcrm.toDF()

df_merge_avayakcrm = df_datajoin02.join(
    df_avayakcrm,
    (df_datajoin02.lista_fecha == df_avayakcrm.lista_avayakcrm_fecha) &
    (df_datajoin02.lista_emp_id == df_avayakcrm.lista_avayakcrm_emp_id)
    , how='outer'
).select(
    df_datajoin02.lista_fecha, df_datajoin02.lista_emp_id, df_datajoin02.lista_avaya_id,
    df_datajoin02.lista_mitrol_id, df_datajoin02.lista_vocalcom_id,
    df_avayakcrm.lista_avayakcrm_id, df_avayakcrm.lista_avayakcrm_fecha,
    df_avayakcrm.lista_avayakcrm_emp_id
)

df_derived_column_avayakcrm_lvl1 = df_merge_avayakcrm.withColumn('tmp_lista_fecha',
                                                                 F.when(df_merge_avayakcrm.lista_fecha.isNull(),
                                                                        df_merge_avayakcrm.lista_avayakcrm_fecha).otherwise(
                                                                     df_merge_avayakcrm.lista_fecha)) \
    .drop(df_merge_avayakcrm.lista_fecha)
df_derived_column_avayakcrm_lvl2 = df_derived_column_avayakcrm_lvl1.withColumn('tmp_lista_emp_id',
                                                                              F.when(
                                                                                  df_derived_column_avayakcrm_lvl1.lista_emp_id.isNull(),
                                                                                  df_derived_column_avayakcrm_lvl1.lista_avayakcrm_emp_id)
                                                                              .otherwise(
                                                                                  df_derived_column_avayakcrm_lvl1.lista_emp_id)) \
    .drop(df_derived_column_avayakcrm_lvl1.lista_emp_id)
df_datajoin03 = df_derived_column_avayakcrm_lvl2.select(
    df_derived_column_avayakcrm_lvl2.tmp_lista_fecha.alias('lista_fecha'),
    df_derived_column_avayakcrm_lvl2.tmp_lista_emp_id.alias('lista_emp_id'),
    df_derived_column_avayakcrm_lvl2.lista_avaya_id,
    df_derived_column_avayakcrm_lvl2.lista_mitrol_id,
    df_derived_column_avayakcrm_lvl2.lista_vocalcom_id,
    df_derived_column_avayakcrm_lvl2.lista_avayakcrm_id)

####### EMPLEADOCUENTA_LUCENT LOGINACD ###############################################################
EmpleadoCuentaLucentloginacd = Filter.apply(
    frame=EmpleadoCuentaLucent,
    f=lambda x: x["empleado_cuenta_lucent_ori_codigo"] == LOGINACD
)
emp_cuenta_lucent_loginacd_df = EmpleadoCuentaLucentloginacd.toDF()
emp_cuenta_lucent_loginacd_df = emp_cuenta_lucent_loginacd_df.withColumn(
    'tmp_empleado_cuenta_lucent_luc_fechahasta',
    F.when(
        emp_cuenta_lucent_loginacd_df.empleado_cuenta_lucent_luc_fechahasta.isNull(),
        datetime.now(TIMEZONE)
    ).otherwise(emp_cuenta_lucent_loginacd_df.empleado_cuenta_lucent_luc_fechahasta)
)

lista_loginacd_join01_df = cal_df.join(
    emp_cuenta_lucent_loginacd_df,
    (cal_df.calendario_fecha >= emp_cuenta_lucent_loginacd_df.empleado_cuenta_lucent_luc_fechadesde) &
    (cal_df.calendario_fecha <= emp_cuenta_lucent_loginacd_df.tmp_empleado_cuenta_lucent_luc_fechahasta)
    , how='inner'
)

lista_loginacd_join01_df = lista_loginacd_join01_df.drop("tmp_empleado_cuenta_lucent_luc_fechahasta")

ListaLoginacdSrc = DynamicFrame.fromDF(
    lista_loginacd_join01_df.select([
        "calendario_fecha",
        "empleado_cuenta_lucent_emp_id",
        "empleado_cuenta_lucent_luc_agente",
        F.row_number().over(
            Window.partitionBy(
                lista_loginacd_join01_df['calendario_fecha'], lista_loginacd_join01_df['empleado_cuenta_lucent_emp_id']
            ).orderBy(
                lista_loginacd_join01_df['empleado_cuenta_lucent_luc_agente'])
        ).alias("fila")
    ]),
    glueContext,
    "ListaLoginacdSrc",
)
ListaLoginacdFila1 = Filter.apply(
    frame=ListaLoginacdSrc,
    f=lambda x: x["fila"] == 1
)
ListaLoginacd = ApplyMapping.apply(
    frame=ListaLoginacdFila1,
    mappings=[
        ("calendario_fecha", "timestamp", "lista_loginacd_fecha", "timestamp"),
        ("empleado_cuenta_lucent_emp_id", "int", "lista_loginacd_emp_id", "int"),
        ("empleado_cuenta_lucent_luc_agente", "int", "lista_loginacd_id", "int"),
        ("fila", "int", "lista_loginacd_fila", "int"),
    ],
    transformation_ctx="ListaLoginacd",
)
ListaLoginacd = DynamicFrame.fromDF(
    ListaLoginacd.toDF().orderBy(["lista_loginacd_fecha", "lista_loginacd_emp_id"], ascending=[1, 1]),
    glueContext,
    "ListaLoginacdSort",
)

####### MERGE CALENDARIO SET VS LOGINACD #######
# df_datajoin03
df_loginacd = ListaLoginacd.toDF()

df_merge_loginacd = df_datajoin03.join(
    df_loginacd,
    (df_datajoin03.lista_fecha == df_loginacd.lista_loginacd_fecha) &
    (df_datajoin03.lista_emp_id == df_loginacd.lista_loginacd_emp_id)
    , how='outer'
).select(
    df_datajoin03.lista_fecha, df_datajoin03.lista_emp_id, df_datajoin03.lista_avaya_id,
    df_datajoin03.lista_mitrol_id, df_datajoin03.lista_vocalcom_id,
    df_datajoin03.lista_avayakcrm_id,
    df_loginacd.lista_loginacd_id, df_loginacd.lista_loginacd_fecha,
    df_loginacd.lista_loginacd_emp_id
)

df_derived_column_loginacd_lvl1 = df_merge_loginacd.withColumn(
    'tmp_lista_fecha',
    F.when(
        df_merge_loginacd.lista_fecha.isNull(),
        df_merge_loginacd.lista_loginacd_fecha
    ).otherwise(df_merge_loginacd.lista_fecha)
).drop(df_merge_loginacd.lista_fecha)

df_derived_column_loginacd_lvl2 = df_derived_column_loginacd_lvl1.withColumn(
    'tmp_lista_emp_id',
    F.when(
        df_derived_column_loginacd_lvl1.lista_emp_id.isNull(),
        df_derived_column_loginacd_lvl1.lista_loginacd_emp_id
    ).otherwise(df_derived_column_loginacd_lvl1.lista_emp_id)
).drop(df_derived_column_loginacd_lvl1.lista_emp_id)

df_datajoin04 = df_derived_column_loginacd_lvl2.select(
    df_derived_column_loginacd_lvl2.tmp_lista_fecha.alias('lista_fecha'),
    df_derived_column_loginacd_lvl2.tmp_lista_emp_id.alias('lista_emp_id'),
    df_derived_column_loginacd_lvl2.lista_avaya_id,
    df_derived_column_loginacd_lvl2.lista_mitrol_id,
    df_derived_column_loginacd_lvl2.lista_vocalcom_id,
    df_derived_column_loginacd_lvl2.lista_avayakcrm_id,
    df_derived_column_loginacd_lvl2.lista_loginacd_id
)

####### EMPLEADOCUENTA_LUCENT NEOTEL ###############################################################
EmpleadoCuentaLucentneotel = Filter.apply(
    frame=EmpleadoCuentaLucent,
    f=lambda x: x["empleado_cuenta_lucent_ori_codigo"] == NEOTEL
)
emp_cuenta_lucent_neotel_df = EmpleadoCuentaLucentneotel.toDF()
emp_cuenta_lucent_neotel_df = emp_cuenta_lucent_neotel_df.withColumn(
    'tmp_empleado_cuenta_lucent_luc_fechahasta',
    F.when(
        emp_cuenta_lucent_neotel_df.empleado_cuenta_lucent_luc_fechahasta.isNull(),
        datetime.now(TIMEZONE)
    ).otherwise(emp_cuenta_lucent_neotel_df.empleado_cuenta_lucent_luc_fechahasta)
)

lista_neotel_join01_df = cal_df.join(
    emp_cuenta_lucent_neotel_df,
    (cal_df.calendario_fecha >= emp_cuenta_lucent_neotel_df.empleado_cuenta_lucent_luc_fechadesde) &
    (cal_df.calendario_fecha <= emp_cuenta_lucent_neotel_df.tmp_empleado_cuenta_lucent_luc_fechahasta)
    , how='inner'
)

lista_neotel_join01_df = lista_neotel_join01_df.drop("tmp_empleado_cuenta_lucent_luc_fechahasta")

ListaneotelSrc = DynamicFrame.fromDF(
    lista_neotel_join01_df.select(
        [
            "calendario_fecha",
            "empleado_cuenta_lucent_emp_id",
            "empleado_cuenta_lucent_luc_agente",
            F.row_number().over(
                Window.partitionBy(
                    lista_neotel_join01_df['calendario_fecha'], lista_neotel_join01_df['empleado_cuenta_lucent_emp_id']
                ).orderBy(lista_neotel_join01_df['empleado_cuenta_lucent_luc_agente'])
            ).alias("fila")
        ]
    ),
    glueContext,
    "ListaneotelSrc",
)
ListaneotelFila1 = Filter.apply(
    frame=ListaneotelSrc,
    f=lambda x: x["fila"] == 1
)
Listaneotel = ApplyMapping.apply(
    frame=ListaneotelFila1,
    mappings=[
        ("calendario_fecha", "timestamp", "lista_neotel_fecha", "timestamp"),
        ("empleado_cuenta_lucent_emp_id", "int", "lista_neotel_emp_id", "int"),
        ("empleado_cuenta_lucent_luc_agente", "int", "lista_neotel_id", "int"),
        ("fila", "int", "lista_neotel_fila", "int"),
    ],
    transformation_ctx="Listaneotel",
)
Listaneotel = DynamicFrame.fromDF(
    Listaneotel.toDF().orderBy(["lista_neotel_fecha", "lista_neotel_emp_id"], ascending=[1, 1]),
    glueContext,
    "ListaneotelSort",
)

####### MERGE CALENDARIO SET VS NEOTEL #######
# df_datajoin04
df_neotel = Listaneotel.toDF()

df_merge_neotel = df_datajoin04.join(
    df_neotel,
    (df_datajoin04.lista_fecha == df_neotel.lista_neotel_fecha) &
    (df_datajoin04.lista_emp_id == df_neotel.lista_neotel_emp_id)
    , how='outer'
).select(
    df_datajoin04.lista_fecha, df_datajoin04.lista_emp_id, df_datajoin04.lista_avaya_id,
    df_datajoin04.lista_mitrol_id, df_datajoin04.lista_vocalcom_id,
    df_datajoin04.lista_avayakcrm_id,
    df_datajoin04.lista_loginacd_id, df_neotel.lista_neotel_id, df_neotel.lista_neotel_fecha,
    df_neotel.lista_neotel_emp_id
)

df_derived_column_neotel_lvl1 = df_merge_neotel.withColumn(
    'tmp_lista_fecha',
    F.when(
        df_merge_neotel.lista_fecha.isNull(),
        df_merge_neotel.lista_neotel_fecha
    ).otherwise(df_merge_neotel.lista_fecha)
).drop(df_merge_neotel.lista_fecha)

df_derived_column_neotel_lvl2 = df_derived_column_neotel_lvl1.withColumn(
    'tmp_lista_emp_id',
    F.when(
        df_derived_column_neotel_lvl1.lista_emp_id.isNull(),
        df_derived_column_neotel_lvl1.lista_neotel_emp_id
    ).otherwise(df_derived_column_neotel_lvl1.lista_emp_id)
).drop(df_derived_column_neotel_lvl1.lista_emp_id)

df_usuarioslucent = df_derived_column_neotel_lvl2.select(
    df_derived_column_neotel_lvl2.tmp_lista_fecha.alias('lista_fecha'),
    df_derived_column_neotel_lvl2.tmp_lista_emp_id.alias('lista_emp_id'),
    df_derived_column_neotel_lvl2.lista_avaya_id,
    df_derived_column_neotel_lvl2.lista_mitrol_id,
    df_derived_column_neotel_lvl2.lista_vocalcom_id,
    df_derived_column_neotel_lvl2.lista_avayakcrm_id,
    df_derived_column_loginacd_lvl2.lista_loginacd_id,
    df_derived_column_neotel_lvl2.lista_neotel_id
)

####### EMPLEADO_SISTEMAEXTERNO ###############################################################
empleado_sistemaexternoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_SISTEMA_EXTERNO,
    transformation_ctx="empleado_sistemaexternoSrc",
)
# Select columns empleado_sistemaexterno ***
empleado_sistemaexternoSelectFields = SelectFields.apply(
    frame=empleado_sistemaexternoSrc,
    paths=["se_id", "emp_id", "ese_login", "ese_fechadesde", "ese_fechahasta", "ese_anulado"],
    transformation_ctx="empleado_sistemaexternoSelectFields",
)
# filter empleado_sistemaexterno ***
empleado_sistemaexternoSelectFields = Filter.apply(
    frame=empleado_sistemaexternoSelectFields,
    f=lambda x: x["ese_anulado"] == 0
)
# Rename columns empleado_sistemaexterno ***
empleado_sistemaexterno = ApplyMapping.apply(
    frame=empleado_sistemaexternoSelectFields,
    mappings=[("se_id", "int", "empleado_sistemaexterno_se_id", "int"),
              ("emp_id", "int", "empleado_sistemaexterno_emp_id", "int"),
              ("ese_login", "string", "empleado_sistemaexterno_ese_login", "string"),
              ("ese_fechadesde", "timestamp", "empleado_sistemaexterno_ese_fechadesde", "timestamp"),
              ("ese_fechahasta", "timestamp", "empleado_sistemaexterno_ese_fechahasta", "timestamp"),
              ("ese_anulado", "tinyint", "empleado_sistemaexterno_ese_anulado", "byte"), ],
    transformation_ctx="empleado_sistemaexterno_ApplyMapping",
)
# filter empleado_sistemaexterno != 25 ***
empleado_sistemaexterno_notis25 = Filter.apply(
    frame=empleado_sistemaexterno,
    f=lambda x: x["empleado_sistemaexterno_se_id"] != 25
)
# filter empleado_sistemaexterno == 25 ***
empleado_sistemaexterno_is25 = Filter.apply(
    frame=empleado_sistemaexterno,
    f=lambda x: x["empleado_sistemaexterno_se_id"] == 25
)

####### SISTEMAEXTERNO ###############################################################
sistemaexternoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SISTEMA_EXTERNO,
    transformation_ctx="sistemaexternoSrc",
)
# Select columns sistemaexterno ***
sistemaexternoSelectFields = SelectFields.apply(
    frame=sistemaexternoSrc,
    paths=["se_id", "se_nombre", "se_anulado"],
    transformation_ctx="sistemaexternoSelectFields",
)
# filter sistemaexterno ***
sistemaexternoSelectFields = Filter.apply(
    frame=sistemaexternoSelectFields,
    f=lambda x: x["se_anulado"] == 0
)
# Rename columns sistemaexterno ***
sistemaexterno = ApplyMapping.apply(
    frame=sistemaexternoSelectFields,
    mappings=[("se_id", "int", "sistemaexterno_se_id", "int"),
              ("se_nombre", "string", "sistemaexterno_se_nombre", "string"),
              ("se_anulado", "tinyint", "sistemaexterno_se_anulado", "byte"), ],
    transformation_ctx="sistemaexterno_ApplyMapping",
)

####### MERGE EMPLEADO_SISTEMAEXTERNO VS SISTEMAEXTERNO  ###############################################################
df_empleado_sistemaexterno_notis25 = empleado_sistemaexterno_notis25.toDF()
df_empleado_sistemaexterno_is25 = empleado_sistemaexterno_is25.toDF()
df_sistemaexterno = sistemaexterno.toDF()
# Merge notis25
df_merge_notis25 = df_empleado_sistemaexterno_notis25.join(
    df_sistemaexterno,
    (df_empleado_sistemaexterno_notis25.empleado_sistemaexterno_se_id == df_sistemaexterno.sistemaexterno_se_id)
    , how='inner'
).select(
    df_empleado_sistemaexterno_notis25.empleado_sistemaexterno_emp_id,
    df_sistemaexterno.sistemaexterno_se_nombre,
    df_empleado_sistemaexterno_notis25.empleado_sistemaexterno_ese_login,
    df_empleado_sistemaexterno_notis25.empleado_sistemaexterno_ese_fechadesde,
    df_empleado_sistemaexterno_notis25.empleado_sistemaexterno_ese_fechahasta
)
# Merge is25
df_merge_is25 = df_empleado_sistemaexterno_is25.join(
    df_sistemaexterno,
    (df_empleado_sistemaexterno_is25.empleado_sistemaexterno_se_id == df_sistemaexterno.sistemaexterno_se_id)
    , how='inner'
).select(
    df_empleado_sistemaexterno_is25.empleado_sistemaexterno_emp_id,
    df_sistemaexterno.sistemaexterno_se_nombre,
    df_empleado_sistemaexterno_is25.empleado_sistemaexterno_ese_login,
    df_empleado_sistemaexterno_is25.empleado_sistemaexterno_ese_fechadesde
)
# Group by dataframe
df_merge_is25 = df_merge_is25.groupBy(
    ["empleado_sistemaexterno_emp_id", "sistemaexterno_se_nombre", "empleado_sistemaexterno_ese_login"]
).agg(
    F.min("empleado_sistemaexterno_ese_fechadesde").alias("empleado_sistemaexterno_ese_fechadesde")
).withColumn("empleado_sistemaexterno_ese_fechahasta", F.lit(None))
# Union dataframe
df_union = df_merge_notis25.union(df_merge_is25)
# Pivot dataframe
se_nombre = ["Amadeus", "Stc", "Adecsys", "Edomus", "Sga", "Vantive", "Genesys Entel", "CTI", "SRM", "FUNCIONARIO",
             "RUT MOVISTAR CHILE DV", "CITRIX MOVISTAR CHILE",
             "UNIFICA MOVISTAR CHILE", "GENESYS MOVISTAR CHILE", "ID SEGURIDAD MOVISTAR CHILE", "AVAYA SUPERVISOR",
             "RRSS MOVISTAR CHILE", "EDELNOR", "BP", "EPR", "RT4",
             "PCE", "TOOLBOX", "Altitude", "FASE", "GRUPO", "MERCADO", "SINE", "SIEBEL", "T3", "REMEDY", "ALTAMIRA",
             "SIMTA", "TOKEN", "SISCEL", "CHAT", "VM", "DISCADOR", "LEGAJO",
             "MAS SIMPLE", "FechaIngreso Operacion", "CMS SUPERVISOR ARG", "ID", "SCUP", "EQUIFAX", "RENIEC", "STC6i",
             "WEB GENIO", "ACADEMY AMDOCS", "WEB CONSULTA PORTABILIDAD",
             "USER_KCRM", "ATIS", "Web de Prevencion", "PSI", "CMS", "OTRS", "WEB CIAT", "SPEEDY SIG", "FENIX",
             "INFODOC", "GESTEL", "PDM", "WEB UNIFICADA", "IRIS", "GENESYS ENTEL PERU",
             "TASA", "TGESTIONA", "WEB UP FRONT", "MOVISTAR T_AYUDA", "PORTAL ENTEL PERU", "PDR",
             "WEB FINANCIAMIENTO CFM", "Clear View", "Web Multiconsulta", "INTRAWEY2", "TOA",
             "EMAIL SPAMINA", "EMAIL GMAIL", "CALCULADORA ARPU", "COD_VEND_ATIS", "COD_VEND_CMS", "SIVADAC", "Landing",
             "NUMERO SOCIO", "WEB SIAC", "SODALI", "ALTAMIRA_MOVPERU",
             "CALCULADORA MOVIL", "WEB DELIVERY", "WDE", "RutAgentSelected", "CITRIX", "VISOR TO BE", "LITHIUM", "CRM",
             "Genesys_VTR", "GenesysCloud BBVA"]
df_pivot = df_union.groupBy(
    ["empleado_sistemaexterno_emp_id", "empleado_sistemaexterno_ese_fechadesde",
     "empleado_sistemaexterno_ese_fechahasta"]
).pivot("sistemaexterno_se_nombre", se_nombre).agg(F.first("empleado_sistemaexterno_ese_login"))
# Rename columns
df_renamecolumns = df_pivot \
    .withColumnRenamed("empleado_sistemaexterno_emp_id", "emp_id") \
    .withColumnRenamed("empleado_sistemaexterno_ese_fechadesde", "fecha_desde") \
    .withColumnRenamed("empleado_sistemaexterno_ese_fechahasta", "fecha_hasta")
df_usuarios = df_renamecolumns.withColumn(
    'tmp_fecha_hasta',
    F.when(
        df_renamecolumns.fecha_hasta.isNull(), datetime.now(TIMEZONE)
    ).otherwise(df_renamecolumns.fecha_hasta)
)

####### MERGE USUARIOS VS CALENDARIO  ###############################################################
df_calendario = Calendario.toDF()
df_usuarios_fechas = df_calendario.join(
    df_usuarios,
    (df_calendario.calendario_fecha >= df_usuarios.fecha_desde) &
    (df_calendario.calendario_fecha <= df_usuarios.tmp_fecha_hasta)
    , how='inner'
)
df_usuarios_fechas = df_usuarios_fechas.drop("tmp_fecha_hasta")

df_usuariosexternos = df_usuarios_fechas.groupBy(["calendario_fecha", "emp_id"]).agg(
    {"Amadeus": "max", "Stc": "max", "Adecsys": "max", "Edomus": "max", "Sga": "max",
     "Vantive": "max", "Genesys Entel": "max", "CTI": "max", "SRM": "max", "FUNCIONARIO": "max",
     "RUT MOVISTAR CHILE DV": "max", "CITRIX MOVISTAR CHILE": "max", "UNIFICA MOVISTAR CHILE": "max",
     "GENESYS MOVISTAR CHILE": "max", "ID SEGURIDAD MOVISTAR CHILE": "max",
     "AVAYA SUPERVISOR": "max", "RRSS MOVISTAR CHILE": "max", "EDELNOR": "max", "BP": "max", "EPR": "max",
     "RT4": "max", "PCE": "max", "TOOLBOX": "max", "Altitude": "max", "FASE": "max", "GRUPO": "max",
     "MERCADO": "max", "SINE": "max", "SIEBEL": "max", "T3": "max", "REMEDY": "max",
     "ALTAMIRA": "max", "SIMTA": "max", "TOKEN": "max", "SISCEL": "max", "CHAT": "max",
     "VM": "max", "DISCADOR": "max", "LEGAJO": "max", "MAS SIMPLE": "max", "FechaIngreso Operacion": "max",
     "CMS SUPERVISOR ARG": "max", "ID": "max", "SCUP": "max", "EQUIFAX": "max", "RENIEC": "max",
     "STC6i": "max", "WEB GENIO": "max", "ACADEMY AMDOCS": "max", "WEB CONSULTA PORTABILIDAD": "max",
     "USER_KCRM": "max",
     "ATIS": "max", "Web de Prevencion": "max", "PSI": "max", "CMS": "max", "OTRS": "max",
     "WEB CIAT": "max", "SPEEDY SIG": "max", "FENIX": "max", "INFODOC": "max", "GESTEL": "max",
     "PDM": "max", "WEB UNIFICADA": "max", "IRIS": "max", "GENESYS ENTEL PERU": "max", "TASA": "max",
     "TGESTIONA": "max", "WEB UP FRONT": "max", "MOVISTAR T_AYUDA": "max", "PORTAL ENTEL PERU": "max", "PDR": "max",
     "WEB FINANCIAMIENTO CFM": "max", "Clear View": "max", "Web Multiconsulta": "max", "INTRAWEY2": "max", "TOA": "max",
     "EMAIL SPAMINA": "max", "EMAIL GMAIL": "max", "CALCULADORA ARPU": "max", "COD_VEND_ATIS": "max",
     "COD_VEND_CMS": "max",
     "SIVADAC": "max", "Landing": "max", "NUMERO SOCIO": "max", "WEB SIAC": "max", "SODALI": "max",
     "ALTAMIRA_MOVPERU": "max", "CALCULADORA MOVIL": "max", "WEB DELIVERY": "max", "WDE": "max",
     "RutAgentSelected": "max",
     "CITRIX": "max", "VISOR TO BE": "max", "LITHIUM": "max", "CRM": "max", "Genesys_VTR": "max",
     "GenesysCloud BBVA": "max"})

####### MERGE USUARIOSLUCENT VS USAURIOSEXTERNOS  ###############################################################
df_merge_usuarios = df_usuarioslucent.join(
    df_usuariosexternos,
    (df_usuarioslucent.lista_fecha == df_usuariosexternos.calendario_fecha) &
    (df_usuarioslucent.lista_emp_id == df_usuariosexternos.emp_id)
    , how='outer'
)

df_derived_column_fecha_lvl1 = df_merge_usuarios.withColumn(
    'tmp_lista_fecha',
    F.when(df_merge_usuarios.lista_fecha.isNull(),
          df_merge_usuarios.calendario_fecha).otherwise(
        df_merge_usuarios.lista_fecha)
).drop(df_merge_usuarios.lista_fecha).drop(df_merge_usuarios.calendario_fecha)

df_derived_column_emp_id_lvl2 = df_derived_column_fecha_lvl1.withColumn(
    'tmp_lista_emp_id',
    F.when(
        df_derived_column_fecha_lvl1.lista_emp_id.isNull(),
        df_derived_column_fecha_lvl1.emp_id).otherwise(
        df_derived_column_fecha_lvl1.lista_emp_id)
).drop(df_derived_column_fecha_lvl1.lista_emp_id).drop(df_derived_column_fecha_lvl1.emp_id)
# convertir fecha a date
df_derived_column_emp_id_lvl2 = df_derived_column_emp_id_lvl2.withColumn(
    "tmp_lista_fecha",
    df_derived_column_emp_id_lvl2["tmp_lista_fecha"].cast(DateType())
)
# renombrar columnas
df_maestro_usuarios = df_derived_column_emp_id_lvl2.select(
    df_derived_column_emp_id_lvl2.tmp_lista_fecha.alias('fecha'),
    df_derived_column_emp_id_lvl2.tmp_lista_emp_id.alias('meucci_id'),
    df_derived_column_emp_id_lvl2.lista_avaya_id.alias('avaya_id'),
    df_derived_column_emp_id_lvl2.lista_mitrol_id.alias('mitrol_id'),
    df_derived_column_emp_id_lvl2.lista_vocalcom_id.alias('vocalcom_id'),
    df_derived_column_emp_id_lvl2.lista_avayakcrm_id.alias('avaya_kcrm_id'),
    df_derived_column_emp_id_lvl2.lista_loginacd_id.alias('login_acd_id'),
    df_derived_column_emp_id_lvl2["max(Amadeus)"].alias('amadeus'),
    df_derived_column_emp_id_lvl2["max(Stc)"].alias('stc'),
    df_derived_column_emp_id_lvl2["max(Adecsys)"].alias('adecsys'),
    df_derived_column_emp_id_lvl2["max(Edomus)"].alias('edomus'),
    df_derived_column_emp_id_lvl2["max(Sga)"].alias('sga'),
    df_derived_column_emp_id_lvl2["max(Vantive)"].alias('vantive'),
    df_derived_column_emp_id_lvl2["max(Genesys Entel)"].alias('genesys_entel'),
    df_derived_column_emp_id_lvl2["max(CTI)"].alias('cti'),
    df_derived_column_emp_id_lvl2["max(SRM)"].alias('srm'),
    df_derived_column_emp_id_lvl2["max(FUNCIONARIO)"].alias('funcionario'),
    df_derived_column_emp_id_lvl2["max(RUT MOVISTAR CHILE DV)"].alias('rut_movistar_chile_dv'),
    df_derived_column_emp_id_lvl2["max(CITRIX MOVISTAR CHILE)"].alias('citrix_movistar_chile'),
    df_derived_column_emp_id_lvl2["max(UNIFICA MOVISTAR CHILE)"].alias('unifica_movistar_chile'),
    df_derived_column_emp_id_lvl2["max(GENESYS MOVISTAR CHILE)"].alias('genesys_movistar_chile'),
    df_derived_column_emp_id_lvl2["max(ID SEGURIDAD MOVISTAR CHILE)"].alias('id_seguridad_movistar_chile'),
    df_derived_column_emp_id_lvl2["max(AVAYA SUPERVISOR)"].alias('avaya_supervisor'),
    df_derived_column_emp_id_lvl2["max(RRSS MOVISTAR CHILE)"].alias('rrss_movistar_chile'),
    df_derived_column_emp_id_lvl2["max(EDELNOR)"].alias('edelnor'),
    df_derived_column_emp_id_lvl2["max(BP)"].alias('bp'),
    df_derived_column_emp_id_lvl2["max(EPR)"].alias('epr'),
    df_derived_column_emp_id_lvl2["max(RT4)"].alias('rt4'),
    df_derived_column_emp_id_lvl2["max(PCE)"].alias('pce'),
    df_derived_column_emp_id_lvl2["max(ToolBox)"].alias('tool_box'),
    df_derived_column_emp_id_lvl2["max(Altitude)"].alias('altitude'),
    df_derived_column_emp_id_lvl2["max(Fase)"].alias('fase'),
    df_derived_column_emp_id_lvl2["max(Grupo)"].alias('grupo'),
    df_derived_column_emp_id_lvl2["max(Mercado)"].alias('mercado'),
    df_derived_column_emp_id_lvl2["max(Sine)"].alias('sine'),
    df_derived_column_emp_id_lvl2["max(Siebel)"].alias('siebel'),
    df_derived_column_emp_id_lvl2["max(T3)"].alias('t3'),
    df_derived_column_emp_id_lvl2["max(Remedy)"].alias('remedy'),
    df_derived_column_emp_id_lvl2["max(Altamira)"].alias('altamira'),
    df_derived_column_emp_id_lvl2["max(Simta)"].alias('simta'),
    df_derived_column_emp_id_lvl2["max(Token)"].alias('token'),
    df_derived_column_emp_id_lvl2["max(Siscel)"].alias('siscel'),
    df_derived_column_emp_id_lvl2["max(Chat)"].alias('chat'),
    df_derived_column_emp_id_lvl2["max(Vm)"].alias('vm'),
    df_derived_column_emp_id_lvl2["max(Discador)"].alias('discador'),
    df_derived_column_emp_id_lvl2["max(Legajo)"].alias('legajo'),
    df_derived_column_emp_id_lvl2["max(MAS SIMPLE)"].alias('mas_simple'),
    df_derived_column_emp_id_lvl2["max(FechaIngreso Operacion)"].alias('fecha_ingreso_operacion'),
    df_derived_column_emp_id_lvl2["max(CMS SUPERVISOR ARG)"].alias('cms_supervisor_arg'),
    df_derived_column_emp_id_lvl2["max(Id)"].alias('id'),
    df_derived_column_emp_id_lvl2["max(Scup)"].alias('scup'),
    df_derived_column_emp_id_lvl2["max(Equifax)"].alias('equifax'),
    df_derived_column_emp_id_lvl2["max(Reniec)"].alias('reniec'),
    df_derived_column_emp_id_lvl2["max(Stc6i)"].alias('stc6i'),
    df_derived_column_emp_id_lvl2["max(WEB GENIO)"].alias('webgenio'),
    df_derived_column_emp_id_lvl2["max(ACADEMY AMDOCS)"].alias('academy_amdocs'),
    df_derived_column_emp_id_lvl2["max(WEB CONSULTA PORTABILIDAD)"].alias('web_consulta_portabilidad'),
    df_derived_column_emp_id_lvl2["max(USER_KCRM)"].alias('user_kcrm'),
    df_derived_column_emp_id_lvl2["max(Atis)"].alias('atis'),
    df_derived_column_emp_id_lvl2["max(Web de Prevencion)"].alias('web_prevencion'),
    df_derived_column_emp_id_lvl2["max(Psi)"].alias('psi'),
    df_derived_column_emp_id_lvl2["max(Cms)"].alias('cms'),
    df_derived_column_emp_id_lvl2["max(Otrs)"].alias('otrs'),
    df_derived_column_emp_id_lvl2["max(WEB CIAT)"].alias('web_ciat'),
    df_derived_column_emp_id_lvl2["max(SPEEDY SIG)"].alias('speedy_sig'),
    df_derived_column_emp_id_lvl2["max(Fenix)"].alias('fenix'),
    df_derived_column_emp_id_lvl2["max(INFODOC)"].alias('infodoc'),
    df_derived_column_emp_id_lvl2["max(Gestel)"].alias('gestel'),
    df_derived_column_emp_id_lvl2["max(Pdm)"].alias('pdm'),
    df_derived_column_emp_id_lvl2["max(WEB UNIFICADA)"].alias('web_unificada'),
    df_derived_column_emp_id_lvl2["max(Iris)"].alias('iris'),
    df_derived_column_emp_id_lvl2["max(GENESYS ENTEL PERU)"].alias('genesys_entel_peru'),
    df_derived_column_emp_id_lvl2["max(Tasa)"].alias('tasa'),
    df_derived_column_emp_id_lvl2["max(TGESTIONA)"].alias('t_gestiona'),
    df_derived_column_emp_id_lvl2["max(WEB UP FRONT)"].alias('web_up_front'),
    df_derived_column_emp_id_lvl2["max(MOVISTAR T_AYUDA)"].alias('movistar_t_ayuda'),
    df_derived_column_emp_id_lvl2["max(PORTAL ENTEL PERU)"].alias('portal_entel_peru'),
    df_derived_column_emp_id_lvl2["max(PDR)"].alias('pdr'),
    df_derived_column_emp_id_lvl2["max(WEB FINANCIAMIENTO CFM)"].alias('web_financiamiento_cfm'),
    df_derived_column_emp_id_lvl2["max(Clear View)"].alias('clear_view'),
    df_derived_column_emp_id_lvl2["max(Web Multiconsulta)"].alias('web_multiconsulta'),
    df_derived_column_emp_id_lvl2["max(INTRAWEY2)"].alias('intrawey2'),
    df_derived_column_emp_id_lvl2["max(TOA)"].alias('toa'),
    df_derived_column_emp_id_lvl2["max(EMAIL SPAMINA)"].alias('email_spamina'),
    df_derived_column_emp_id_lvl2["max(EMAIL GMAIL)"].alias('email_gmail'),
    df_derived_column_emp_id_lvl2["max(CALCULADORA ARPU)"].alias('calculadora_arpu'),
    df_derived_column_emp_id_lvl2["max(COD_VEND_ATIS)"].alias('cod_vend_atis'),
    df_derived_column_emp_id_lvl2["max(COD_VEND_CMS)"].alias('cod_vend_cms'),
    df_derived_column_emp_id_lvl2["max(Sivadac)"].alias('sivadac'),
    df_derived_column_emp_id_lvl2["max(Landing)"].alias('landing'),
    df_derived_column_emp_id_lvl2["max(NUMERO SOCIO)"].alias('numero_socio'),
    df_derived_column_emp_id_lvl2["max(WEB SIAC)"].alias('web_siac'),
    df_derived_column_emp_id_lvl2["max(Sodali)"].alias('sodali'),
    df_derived_column_emp_id_lvl2["max(ALTAMIRA_MOVPERU)"].alias('altamira_mov_peru'),
    df_derived_column_emp_id_lvl2["max(CALCULADORA MOVIL)"].alias('calculadora_movil'),
    df_derived_column_emp_id_lvl2["max(WEB DELIVERY)"].alias('web_delivery'),
    df_derived_column_emp_id_lvl2["max(WDE)"].alias('wde'),
    df_derived_column_emp_id_lvl2["max(RutAgentSelected)"].alias('rut_agent_selected'),
    df_derived_column_emp_id_lvl2["max(CITRIX)"].alias('citrix'),
    df_derived_column_emp_id_lvl2["max(VISOR TO BE)"].alias('visor_to_be'),
    df_derived_column_emp_id_lvl2.lista_neotel_id.alias('neotel'),
    df_derived_column_emp_id_lvl2["max(LITHIUM)"].alias('lithium'),
    df_derived_column_emp_id_lvl2["max(CRM)"].alias('crm'),
    df_derived_column_emp_id_lvl2["max(Genesys_VTR)"].alias('genesys_vtr'),
    df_derived_column_emp_id_lvl2["max(GenesysCloud BBVA)"].alias('genesys_cloud_bbva')
)

####### CREAR DATAFRAME MAESTRO DE USUARIOS  ###############################################################
Result = DynamicFrame.fromDF(df_maestro_usuarios, glueContext, "df_maestro_usuarios_dynamicframe")

# ==================
# BORRAR PARTICIONES
# ==================
FECHA_INI_DATE = datetime.now(TIMEZONE).date() - timedelta(days=TARGET_DAYS)
FECHA_FIN_DATE = datetime.now(TIMEZONE).date()
try:
    # Verifica la existencia de la tabla
    response = glue_client.get_table(
        DatabaseName=TARGET_DATABASE,
        Name=TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS
    )
    print(f'La tabla {TARGET_DATABASE}.{TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS} existe en el catálogo de Glue.')
    validate = 1
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        print(f'La tabla {TARGET_DATABASE}.{TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS} no existe en el catálogo de Glue.')
        validate = 0
    else:
        print(f'Error al verificar la tabla: {e.response["Error"]["Message"]}')
        sys.exit(1)

if validate == 1:
    def split_s3_path(s3_path):
        path_parts = s3_path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)
        return bucket, key
    
    def get_partitions_where_fecha_between(database: str, table: str, _from: date, _to: date):
        _partitions = []
        _expression = f"fecha between '{_from}' and '{_to}'"
        print(f"Get partition from {TARGET_DATABASE}.{TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS} where {_expression}")
        response = glue_client.get_partitions(
            DatabaseName=database,
            TableName=table,
            Expression=_expression,
        )
        _partitions = _partitions + response["Partitions"]
    
        while "NextToken" in response:
            response = glue_client.get_partitions(
                DatabaseName=database,
                TableName=table,
                Expression=_expression,
                NextToken=response["NextToken"]
            )
            _partitions = _partitions + response["Partitions"]
    
        return _partitions
    
    
    def delete_partitions_data_where_fecha_between(database, table, _from: date, _to: date):
        partitions = get_partitions_where_fecha_between(database=database, table=table, _from=_from, _to=_to)
        for partition in partitions:
            _storage_location = partition["StorageDescriptor"]["Location"]
            print(f"delete partition {partition['Values']} stored in {_storage_location}")
    
            _bucket, _key = split_s3_path(_storage_location)
            _objects = s3_client.list_objects_v2(Bucket=_bucket, Prefix=_key)
    
            if 'Contents' in _objects:
                for _object in _objects['Contents']:
                    print('Deleting', _object['Key'])
                    s3_client.delete_object(Bucket=_bucket, Key=_object['Key'])
            else:
                print("No data in partition")
    
    delete_partitions_data_where_fecha_between(database=TARGET_DATABASE,
                                              table=TARGET_TABLE_MAESTRO_EQUIPOS_USUARIOS,
                                              _from=FECHA_INI_DATE,
                                              _to=FECHA_FIN_DATE)

# ==================
# CARGAR PARTICIONES
# ==================
target_maestro_equipos_usuarios_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Result,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": ["fecha"]},
    format_options={"compression": "snappy"},
    transformation_ctx="target_maestro_equipos_usuarios_node3",
)


job.commit()
