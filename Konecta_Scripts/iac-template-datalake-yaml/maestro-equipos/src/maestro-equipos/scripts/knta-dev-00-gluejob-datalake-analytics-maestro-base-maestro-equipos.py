import sys
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
import boto3
import pytz

args = getResolvedOptions(sys.argv, ["JOB_NAME",    
    'AWS_REGION',
    'SOURCE_DATABASE',
    'SOURCE_TABLE_CALENDARIO',
    'SOURCE_TABLE_EMPLEADO_CUENTA',
    'SOURCE_TABLE_EMPLEADO',
    'SOURCE_TABLE_CUENTA',
    'SOURCE_TABLE_SUBAREA',
    'SOURCE_TABLE_CARGO',
    'SOURCE_TABLE_EMPLEADO_SERVICIO',
    'SOURCE_TABLE_SERVICIOS',
    'SOURCE_TABLE_EMPLEADO_MODALIDAD_TRABAJO',
    'SOURCE_TABLE_MODALIDAD_TRABAJO',
    'SOURCE_TABLE_EMPLEADO_SUPERIORES',
    'TARGET_BUCKET',
    'TARGET_DATABASE',
    'TARGET_TABLE_MAESTRO_EQUIPOS',
    'PARAM_INT_DAYS',
    'SOURCE_TABLE_CARGO_TIPO',
    'SOURCE_TABLE_SOCIEDAD',
    'SOURCE_TABLE_SITE',
    'SOURCE_TABLE_EMPLEADO_UBICACION'])
    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_DATABASE = args["SOURCE_DATABASE"]
SOURCE_TABLE_CALENDARIO = args["SOURCE_TABLE_CALENDARIO"]
SOURCE_TABLE_EMPLEADO_CUENTA = args["SOURCE_TABLE_EMPLEADO_CUENTA"]
SOURCE_TABLE_EMPLEADO = args["SOURCE_TABLE_EMPLEADO"]
SOURCE_TABLE_CUENTA = args["SOURCE_TABLE_CUENTA"]
SOURCE_TABLE_SUBAREA = args["SOURCE_TABLE_SUBAREA"]
SOURCE_TABLE_CARGO = args["SOURCE_TABLE_CARGO"]
SOURCE_TABLE_EMPLEADO_SERVICIO = args["SOURCE_TABLE_EMPLEADO_SERVICIO"]
SOURCE_TABLE_SERVICIOS = args["SOURCE_TABLE_SERVICIOS"]
SOURCE_TABLE_EMPLEADO_MODALIDAD_TRABAJO = args["SOURCE_TABLE_EMPLEADO_MODALIDAD_TRABAJO"]
SOURCE_TABLE_MODALIDAD_TRABAJO = args["SOURCE_TABLE_MODALIDAD_TRABAJO"]
SOURCE_TABLE_EMPLEADO_SUPERIORES = args["SOURCE_TABLE_EMPLEADO_SUPERIORES"]
SOURCE_TABLE_CARGO_TIPO = args["SOURCE_TABLE_CARGO_TIPO"]
SOURCE_TABLE_SOCIEDAD = args["SOURCE_TABLE_SOCIEDAD"]
SOURCE_TABLE_SITE = args["SOURCE_TABLE_SITE"]
SOURCE_TABLE_EMPLEADO_UBICACION = args["SOURCE_TABLE_EMPLEADO_UBICACION"]
TARGET_BUCKET = args["TARGET_BUCKET"]
TARGET_DATABASE = args["TARGET_DATABASE"]
TARGET_TABLE_MAESTRO_EQUIPOS = args["TARGET_TABLE_MAESTRO_EQUIPOS"]

TIMEZONE = pytz.timezone('America/Lima')
AWS_REGION = args['AWS_REGION']
print(f"in region {AWS_REGION}")
now = str(datetime.now(TIMEZONE))
TARGET_DAYS = int(args["PARAM_INT_DAYS"])  # 30
FECHA_INI = datetime.combine(datetime.now(TIMEZONE).date() - timedelta(days=TARGET_DAYS), datetime.min.time())
FECHA_FIN = datetime.combine(datetime.now(TIMEZONE).date(), datetime.min.time())
print(f"FECHA_INI:{FECHA_INI} - FECHA_FIN:{FECHA_FIN}")

s3_client = boto3.client('s3', region_name='us-east-1')
glue_client = boto3.client("glue", region_name=AWS_REGION)

# ================ CALENDARIO ================
# ============================================
CalendarioSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CALENDARIO,
    transformation_ctx="CalendarioSrc",
)
# print("==========CalendarioSrc==========")
# CalendarioSrc.printSchema()
# CalendarioSrc.toDF().show(10)
# print("==========================")
ChangeSchema_calendario = ApplyMapping.apply(
frame=CalendarioSrc,
mappings=[
    ("fecha", "timestamp", "fecha", "timestamp"),
    ("lunes", "int", "lunes", "int"),
    ("martes", "int", "martes", "int"),
    ("miercoles", "int", "miercoles", "int"),
    ("jueves", "int", "jueves", "int"),
    ("viernes", "int", "viernes", "int"),
    ("sabado", "int", "sabado", "int"),
    ("domingo", "int", "domingo", "int"),
],
transformation_ctx="ChangeSchema_calendario",
)
CalendarioSelectFields = SelectFields.apply(
    frame=ChangeSchema_calendario,
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
# print("==========Calendario==========")
# Calendario.printSchema()
# Calendario.toDF().show(10)
# print("==========================")

# ================ EMPLEADO CUENTA ================
# =================================================
EmpleadoCuentaSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_CUENTA,
    transformation_ctx="EmpleadoCuentaSrc",
)
# print("==========EmpleadoCuentaSrc==========")
# EmpleadoCuentaSrc.printSchema()
# EmpleadoCuentaSrc.toDF().show(10)
# print("==========================")
EmpleadoCuentaSelectFields = SelectFields.apply(
    frame=EmpleadoCuentaSrc,
    paths=["anulado", "emp_id", "ecu_fechadesde", "ecu_fechahasta", "cue_id", "sar_id", "car_id"],
    transformation_ctx="EmpleadoCuentaSelectFields",
)
EmpleadoCuentaNoAnulado = Filter.apply(
    frame=EmpleadoCuentaSelectFields,
    f=lambda x: x["anulado"] == 0
)
EmpleadoCuenta = ApplyMapping.apply(
    frame=EmpleadoCuentaNoAnulado,
    mappings=[
        ("cue_id", "int", "empleado_cuenta_cue_id", "int"),
        ("ecu_fechadesde", "timestamp", "empleado_cuenta_ecu_fechadesde", "timestamp"),
        ("ecu_fechahasta", "timestamp", "empleado_cuenta_ecu_fechahasta", "timestamp"),
        ("anulado", "int", "empleado_cuenta_anulado", "byte"),
        ("car_id", "int", "empleado_cuenta_car_id", "int"),
        ("emp_id", "int", "empleado_cuenta_emp_id", "int"),
        ("sar_id", "int", "empleado_cuenta_sar_id", "int"),
    ],
    transformation_ctx="EmpleadoCuenta",
)
# print("==========EmpleadoCuenta==========")
# EmpleadoCuenta.printSchema()
# EmpleadoCuenta.toDF().show(10)
# print("==========================")

# ================ EMPLEADO ================
# ==========================================
EmpleadoScr = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO,
    transformation_ctx="EmpleadoScr",
)
# print("==========EmpleadoScr==========")
# EmpleadoScr.printSchema()
# EmpleadoScr.toDF().show(10)
# print("==========================")
EmpleadoSelectFields = SelectFields.apply(
    frame=EmpleadoScr,
    paths=["emp_id", "emp_docnro", "emp_apellido", "emp_nombre", "suc_id", "anulado", "sociedad_id"],
    transformation_ctx="EmpleadoSelectFields",
)
Empleado = ApplyMapping.apply(
    frame=EmpleadoSelectFields,
    mappings=[
        ("suc_id", "int", "empleado_suc_id", "int"),
        ("emp_nombre", "string", "empleado_emp_nombre", "string"),
        ("emp_docnro", "int", "empleado_emp_docnro", "int"),
        ("anulado", "int", "empleado_anulado", "byte"),
        ("emp_apellido", "string", "empleado_emp_apellido", "string"),
        ("emp_id", "int", "empleado_emp_id", "int"),
        ("sociedad_id", "int", "empleado_sociedad_id", "int"),
    ],
    transformation_ctx="Empleado",
)
# print("==========Empleado==========")
# Empleado.printSchema()
# Empleado.toDF().show(10)
# print("==========================")

# ================ CUENTA ================
# ========================================
CuentaSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CUENTA,
    transformation_ctx="CuentaSrc",
)
# print("==========CuentaSrc==========")
# CuentaSrc.printSchema()
# CuentaSrc.toDF().show(10)
# print("==========================")
CuentaSelectFields = SelectFields.apply(
    frame=CuentaSrc,
    paths=["cue_id", "cue_nombre"],
    transformation_ctx="CuentaSelectFields",
)
Cuenta = ApplyMapping.apply(
    frame=CuentaSelectFields,
    mappings=[
        ("cue_id", "int", "cuenta_cue_id", "int"),
        ("cue_nombre", "string", "cuenta_cue_nombre", "string"),
    ],
    transformation_ctx="Cuenta",
)
# print("==========Cuenta==========")
# Cuenta.printSchema()
# Cuenta.toDF().show(10)
# print("==========================")

# ================ SUB_AREA ================
# ==========================================
SubAreaSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SUBAREA,
    transformation_ctx="SubAreaSrc",
)
# print("==========SubAreaSrc==========")
# SubAreaSrc.printSchema()
# SubAreaSrc.toDF().show(10)
# print("==========================")
SubAreaSelectFields = SelectFields.apply(
    frame=SubAreaSrc,
    paths=["sar_id", "sar_nombre", "cue_id"],
    transformation_ctx="SubAreaSelectFields",
)
SubArea = ApplyMapping.apply(
    frame=SubAreaSelectFields,
    mappings=[
        ("sar_nombre", "string", "sub_area_sar_nombre", "string"),
        ("cue_id", "int", "sub_area_cue_id", "int"),
        ("sar_id", "int", "sub_area_sar_id", "int"),
    ],
    transformation_ctx="SubArea",
)
# print("==========SubArea==========")
# SubArea.printSchema()
# SubArea.toDF().show(10)
# print("==========================")

# ================ CARGO ================
# =======================================
CargoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CARGO,
    transformation_ctx="CargoSrc",
)
# print("==========CargoSrc==========")
# CargoSrc.printSchema()
# CargoSrc.toDF().show(10)
# print("==========================")
CargoSelectFields = SelectFields.apply(
    frame=CargoSrc,
    paths=["car_id", "car_nombre", "car_tipo_id"],
    transformation_ctx="CargoSelectFields",
)
Cargo = ApplyMapping.apply(
    frame=CargoSelectFields,
    mappings=[
        ("car_id", "int", "cargo_car_id", "int"),
        ("car_nombre", "string", "cargo_car_nombre", "string"),
        ("car_tipo_id", "int", "cargo_car_tipo_id", "int"),
    ],
    transformation_ctx="Cargo",
)
# print("==========Cargo==========")
# Cargo.printSchema()
# Cargo.toDF().show(10)
# print("==========================")

# ================ EMPLEADO SERVICIO ================
# ===================================================
EmpleadoServicioSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_SERVICIO,
    transformation_ctx="EmpleadoServicioSrc",
)
# print("==========EmpleadoServicioSrc==========")
# EmpleadoServicioSrc.printSchema()
# EmpleadoServicioSrc.toDF().show(10)
# print("==========================")
EmpleadoServicioSelectFields = SelectFields.apply(
    frame=EmpleadoServicioSrc,
    paths=["empsrv_fechadesde", "empsrv_fechahasta", "emp_id", "anulado", "srv_id"],
    transformation_ctx="EmpleadoServicioSelectFields",
)
EmpleadoServicioNoAnulado = Filter.apply(
    frame=EmpleadoServicioSelectFields,
    f=lambda x: x["anulado"] == 0
)
EmpleadoServicio = ApplyMapping.apply(
    frame=EmpleadoServicioNoAnulado,
    mappings=[
        ("srv_id", "int", "empleado_servicio_srv_id", "int"),
        ("empsrv_fechadesde", "timestamp", "empleado_servicio_empsrv_fechadesde", "timestamp",),
        ("empsrv_fechahasta", "timestamp", "empleado_servicio_empsrv_fechahasta", "timestamp",),
        ("anulado", "int", "empleado_servicio_anulado", "int"),
        ("emp_id", "int", "empleado_servicio_emp_id", "int"),
    ],
    transformation_ctx="EmpleadoServicio",
)
# print("==========EmpleadoServicio==========")
# EmpleadoServicio.printSchema()
# EmpleadoServicio.toDF().show(10)
# print("==========================")
# ================ SERVICIOS ================
# ===========================================
ServiciosSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SERVICIOS,
    transformation_ctx="ServiciosSrc",
)
# print("==========ServiciosSrc==========")
# ServiciosSrc.printSchema()
# ServiciosSrc.toDF().show(10)
# print("==========================")
ServiciosSelectFields = SelectFields.apply(
    frame=ServiciosSrc,
    paths=["srv_id", "srv_nombre"],
    transformation_ctx="ServiciosSelectFields",
)
Servicios = ApplyMapping.apply(
    frame=ServiciosSelectFields,
    mappings=[
        ("srv_nombre", "string", "servicios_srv_nombre", "string"),
        ("srv_id", "int", "servicios_srv_id", "int"),
    ],
    transformation_ctx="Servicios",
)
# print("==========Servicios==========")
# Servicios.printSchema()
# Servicios.toDF().show(10)
# print("==========================")
# ================ EMPLEADO_MODALIDAD_TRABAJO ================
# ============================================================
EmpleadoModalidadTrabajoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_MODALIDAD_TRABAJO,
    transformation_ctx="EmpleadoModalidadTrabajoSrc",
)
# print("==========EmpleadoModalidadTrabajoSrc==========")
# EmpleadoModalidadTrabajoSrc.printSchema()
# EmpleadoModalidadTrabajoSrc.toDF().show(10)
# print("==========================")

EmpleadoModalidadTrabajoSelectFields = SelectFields.apply(
    frame=EmpleadoModalidadTrabajoSrc,
    paths=["emp_id", "anulado", "id_mod_trab", "fechadesde", "fechahasta"],
    transformation_ctx="EmpleadoModalidadTrabajoSelectFields",
)
EmpleadoModalidadTrabajoNoAnulado = Filter.apply(
    frame=EmpleadoModalidadTrabajoSelectFields,
    f=lambda x: x["anulado"] == 0
)
EmpleadoModalidadTrabajo = ApplyMapping.apply(
    frame=EmpleadoModalidadTrabajoNoAnulado,
    mappings=[
        ("fechadesde", "timestamp", "empleado_modalidad_trabajo_fechadesde", "timestamp",),
        ("fechahasta", "timestamp", "empleado_modalidad_trabajo_fechahasta", "timestamp",),
        ("id_mod_trab", "int", "empleado_modalidad_trabajo_id_mod_trab", "int"),
        ("anulado", "int", "empleado_modalidad_trabajo_anulado", "int"),
        ("emp_id", "int", "empleado_modalidad_trabajo_emp_id", "int"),
    ],
    transformation_ctx="EmpleadoModalidadTrabajo",
)
# print("==========EmpleadoModalidadTrabajo==========")
# EmpleadoModalidadTrabajo.printSchema()
# EmpleadoModalidadTrabajo.toDF().show(10)
# print("==========================")

# ================ MODALIDAD_TRABAJO ================
# ===================================================
ModalidadTrabajoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_MODALIDAD_TRABAJO,
    transformation_ctx="ModalidadTrabajoSrc",
)
# print("==========ModalidadTrabajoSrc==========")
# ModalidadTrabajoSrc.printSchema()
# ModalidadTrabajoSrc.toDF().show(10)
# print("==========================")
ModalidadTrabajoSelectFields = SelectFields.apply(
    frame=ModalidadTrabajoSrc,
    paths=["modalidad", "id_mod_trab", "anulado"],
    transformation_ctx="ModalidadTrabajoSelectFields",
)
ModalidadTrabajoNoAnulado = Filter.apply(
    frame=ModalidadTrabajoSelectFields,
    f=lambda x: x["anulado"] == 0
)
ModalidadTrabajo = ApplyMapping.apply(
    frame=ModalidadTrabajoNoAnulado,
    mappings=[
        ("anulado", "int", "modalidad_trabajo_anulado", "int"),
        ("modalidad", "string", "modalidad_trabajo_modalidad", "string"),
        ("id_mod_trab", "int", "modalidad_trabajo_id_mod_trab", "int"),
    ],
    transformation_ctx="ModalidadTrabajo",
)
# print("==========ModalidadTrabajo==========")
# ModalidadTrabajo.printSchema()
# ModalidadTrabajo.toDF().show(10)
# print("==========================")

# ================ EMPLEADO_SUPERIORES ================
# =====================================================
EmpleadoSuperioresSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_SUPERIORES,
    transformation_ctx="EmpleadoSuperioresSrc",
)
# print("==========EmpleadoSuperioresSrc==========")
# EmpleadoSuperioresSrc.printSchema()
# EmpleadoSuperioresSrc.toDF().show(10)
# print("==========================")
EmpleadoSuperioresSelectFields = SelectFields.apply(
    frame=EmpleadoSuperioresSrc,
    paths=["emp_id", "anulado", "fechadesde", "fechahasta", "emp_idsup"],
    transformation_ctx="EmpleadoSuperioresSelectFields",
)

EmpleadoSuperioresNoAnulado = Filter.apply(
    frame=EmpleadoSuperioresSelectFields,
    f=lambda x: x["anulado"] == 0
)

EmpleadoSuperiores = ApplyMapping.apply(
    frame=EmpleadoSuperioresNoAnulado,
    mappings=[
        ("emp_idsup", "int", "empleado_superiores_emp_idsup", "int"),
        ("fechadesde", "timestamp", "empleado_superiores_fechadesde", "timestamp"),
        ("fechahasta", "timestamp", "empleado_superiores_fechahasta", "timestamp"),
        ("anulado", "byte", "empleado_superiores_anulado", "byte"),
        ("emp_id", "int", "empleado_superiores_emp_id", "int"),
    ],
    transformation_ctx="EmpleadoSuperiores",
)
# print("==========EmpleadoSuperiores==========")
# EmpleadoSuperiores.printSchema()
# EmpleadoSuperiores.toDF().show(10)
# print("==========================")

# ================ CARGO_TIPO ===============
# ===========================================
CargoTipoSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_CARGO_TIPO,
    transformation_ctx="CargoTipoSrc",
)
# print("==========CargoTipoSrc==========")
# CargoTipoSrc.printSchema()
# CargoTipoSrc.toDF().show(10)
# print("==========================")
CargoTipoSelectFields = SelectFields.apply(
    frame=CargoTipoSrc,
    paths=["car_tipo_id", "car_tipo_nombre"],
    transformation_ctx="CargoTipoSelectFields",
)
CargoTipo = ApplyMapping.apply(
    frame=CargoTipoSelectFields,
    mappings=[
        ("car_tipo_nombre", "string", "CargoTipo_car_tipo_nombre", "string"),
        ("car_tipo_id", "int", "CargoTipo_car_tipo_id", "int"),
    ],
    transformation_ctx="CargoTipo",
)
# print("==========CargoTipo==========")
# CargoTipo.printSchema()
# CargoTipo.toDF().show(10)
# print("==========================")

# ================ SOCIEDAD ===============
# =========================================
SociedadSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SOCIEDAD,
    transformation_ctx="SociedadSrc",
)
# print("==========SociedadSrc==========")
# SociedadSrc.printSchema()
# SociedadSrc.toDF().show(10)
# print("==========================")
SociedadSelectFields = SelectFields.apply(
    frame=SociedadSrc,
    paths=["sociedad_id", "sociedad_nombre"],
    transformation_ctx="SociedadSelectFields",
)
Sociedad = ApplyMapping.apply(
    frame=SociedadSelectFields,
    mappings=[
        ("sociedad_nombre", "string", "Sociedad_sociedad_nombre", "string"),
        ("sociedad_id", "int", "Sociedad_sociedad_id", "int"),
    ],
    transformation_ctx="Sociedad",
)
# print("==========Sociedad==========")
# Sociedad.printSchema()
# Sociedad.toDF().show(10)
# print("==========================")

# ================ Site ===============
# =========================================
SiteSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_SITE,
    transformation_ctx="SiteSrc",
)
# print("==========SiteSrc==========")
# SiteSrc.printSchema()
# SiteSrc.toDF().show(10)
# print("==========================")
SiteSelectFields = SelectFields.apply(
    frame=SiteSrc,
    paths=["site_id", "site_nombre"],
    transformation_ctx="SiteSelectFields",
)
Site = ApplyMapping.apply(
    frame=SiteSelectFields,
    mappings=[
        ("site_nombre", "string", "Site_site_nombre", "string"),
        ("site_id", "int", "Site_site_id", "int"),
    ],
    transformation_ctx="Site",
)
# print("==========Site==========")
# Site.printSchema()
# Site.toDF().show(10)
# print("==========================")

# ================ EMPLEADO_UBICACION ===============
# =========================================
EmpleadoUbicacionSrc = glueContext.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE_EMPLEADO_UBICACION,
    transformation_ctx="EmpleadoUbicacionSrc",
)
# print("==========EmpleadoUbicacionSrc==========")
# EmpleadoUbicacionSrc.printSchema()
# EmpleadoUbicacionSrc.toDF().show(10)
# print("==========================")
EmpleadoUbicacionSelectFields = SelectFields.apply(
    frame=EmpleadoUbicacionSrc,
    paths=["emp_id", "fechadesde", "fechahasta", "anulado", "site_id"],
    transformation_ctx="EmpleadoUbicacionSelectFields",
)
EmpleadoUbicacionNoAnulado = Filter.apply(
    frame=EmpleadoUbicacionSelectFields,
    f=lambda x: x["anulado"] == 0
)
EmpleadoUbicacion = ApplyMapping.apply(
    frame=EmpleadoUbicacionNoAnulado,
    mappings=[
        ("emp_id", "int", "EmpleadoUbicacion_emp_id", "int"),
        ("fechadesde", "timestamp", "EmpleadoUbicacion_fechadesde", "timestamp"),
        ("fechahasta", "timestamp", "EmpleadoUbicacion_fechahasta", "timestamp"),
        ("anulado", "byte", "EmpleadoUbicacion_anulado", "byte"),
        ("site_id", "int", "EmpleadoUbicacion_site_id", "int"),
    ],
    transformation_ctx="EmpleadoUbicacion",
)
# print("==========EmpleadoUbicacion==========")
# EmpleadoUbicacion.printSchema()
# EmpleadoUbicacion.toDF().show(10)
# print("==========================")

# | JOINS NIVEL 1|

# ========== Join01 Calendario - EmpleadoCuenta ==========
# ========================================================
cal_df = Calendario.toDF()
emp_df = EmpleadoCuenta.toDF()
emp_df = emp_df.withColumn('tmp_empleado_cuenta_ecu_fechahasta',
                            when(emp_df["empleado_cuenta_ecu_fechahasta"].isNull(), datetime.now()).otherwise(emp_df["empleado_cuenta_ecu_fechahasta"]))

join01_df = cal_df.join(
    emp_df,
    (cal_df.calendario_fecha >= emp_df.empleado_cuenta_ecu_fechadesde) &
    (cal_df.calendario_fecha <= emp_df.tmp_empleado_cuenta_ecu_fechahasta)
    , how='inner')

Join01 = join01_df.drop("tmp_empleado_cuenta_ecu_fechahasta")
# Join01 = DynamicFrame.fromDF(join01_df, glueContext, "Join01")
# print("==========Join01==========")
# Join01.printSchema()
# Join01.show(10)
# print("==========================")

# ========== Join02 (join01 vs Empleado) ====================
# ===========================================================
empleado = Empleado.toDF()
Join02 = Join01.join(empleado,
        (col("empleado_cuenta_emp_id") == col("empleado_emp_id")),
        how="inner")
# print("==========Join02==========")
# Join02.printSchema()
# Join02.show(10)
# print("==========================")

# ========== Join03 (join02 vs Cuenta) ======================
# ===========================================================
cuenta_df = Cuenta.toDF()
Join03 = Join02.join(cuenta_df,
        (Join02["empleado_cuenta_cue_id"] == cuenta_df["cuenta_cue_id"]),
        how="left")
# print("==========Join03==========")
# Join03.printSchema()
# Join03.show(10)
# print("==========================")

# ========== Join04 (join03 vs Subarea) =====================
# ===========================================================
subarea_df = SubArea.toDF()
Join04 = Join03.join(subarea_df,
        (Join03["empleado_cuenta_cue_id"] == subarea_df["sub_area_cue_id"]) &
        (Join03["empleado_cuenta_sar_id"] == subarea_df["sub_area_sar_id"]),
        how="left")
# print("==========Join04==========")
# Join04.printSchema()
# Join04.show(10)
# print("==========================")

# ========== Join05 (join04 vs Cargo) =====================
# =========================================================
cargo_df = Cargo.toDF()
Join05 = Join04.join(cargo_df,
        (Join04["empleado_cuenta_car_id"] == cargo_df["cargo_car_id"]),
        how="left")
# print("==========Join05==========")
# Join05.printSchema()
# Join05.show(10)
# print("==========================")

# ========== Join06 (join05 vs EmpleadoServicio) ==========
# =========================================================
empleado_servicio_df = EmpleadoServicio.toDF()
empleado_servicio_df = empleado_servicio_df.withColumn('tmp_empleado_servicio_empsrv_fechahasta',
                                                        when(empleado_servicio_df["empleado_servicio_empsrv_fechahasta"].isNull(), datetime.now()).otherwise(empleado_servicio_df["empleado_servicio_empsrv_fechahasta"]))

join06_df = Join05.join(empleado_servicio_df,
    (Join05.empleado_emp_id == empleado_servicio_df.empleado_servicio_emp_id) &
    (Join05.calendario_fecha >= empleado_servicio_df.empleado_servicio_empsrv_fechadesde) &
    (Join05.calendario_fecha <= empleado_servicio_df.tmp_empleado_servicio_empsrv_fechahasta)
    , how='left')

Join06 = join06_df.drop("tmp_empleado_servicio_empsrv_fechahasta")
# print("==========Join06==========")
# Join06.printSchema()
# Join06.show(10)
# print("==========================")

# ========== Join07 (join06 vs Servicios) ==========
# ==================================================
servicios_df = Servicios.toDF()
Join07 = Join06.join(servicios_df,
        (Join06["empleado_servicio_srv_id"] == servicios_df["servicios_srv_id"]),
        how="left")
# print("==========Join07==========")
# Join07.printSchema()
# Join07.show(10)
# print("==========================")

# ========== Join08 (join07 vs EmpleadoModalidadTrabajo) ==========
# =================================================================
empleado_modalidad_trabajo_df = EmpleadoModalidadTrabajo.toDF()
empleado_modalidad_trabajo_df = empleado_modalidad_trabajo_df.withColumn('tmp_empleado_modalidad_trabajo_fechahasta',
                                                                        when(empleado_modalidad_trabajo_df["empleado_modalidad_trabajo_fechahasta"].isNull(), datetime.now()).otherwise(empleado_modalidad_trabajo_df["empleado_modalidad_trabajo_fechahasta"]))

join08_df = Join07.join(
    empleado_modalidad_trabajo_df,
    (Join07.empleado_emp_id == empleado_modalidad_trabajo_df.empleado_modalidad_trabajo_emp_id) &
    (Join07.calendario_fecha >= empleado_modalidad_trabajo_df.empleado_modalidad_trabajo_fechadesde) &
    (Join07.calendario_fecha <= empleado_modalidad_trabajo_df.tmp_empleado_modalidad_trabajo_fechahasta)
    , how='left')

Join08 = join08_df.drop("tmp_empleado_modalidad_trabajo_fechahasta")
# print("==========Join08==========")
# empleado_modalidad_trabajo_df.show(10)
# Join08.printSchema()
# Join08.select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("empleado_modalidad_trabajo_id_mod_trab"),col("empleado_modalidad_trabajo_fechadesde"),col("empleado_modalidad_trabajo_fechahasta")).show(10)
# print("==========================")

# ========== Join09 (join08 vs ModalidadTrabajo) ==========
# =========================================================
modalidad_trabajo_df = ModalidadTrabajo.toDF()
Join09 = Join08.join(modalidad_trabajo_df,
        (Join08["empleado_modalidad_trabajo_id_mod_trab"] == modalidad_trabajo_df["modalidad_trabajo_id_mod_trab"]),
        how="left")
# print("==========Join09==========")
# Join08.show(10)
# Join09.printSchema()
# print("==========================")

# ========== Join10 (join09 vs CargoTipo) ==========
# ==================================================
cago_tipo_df = CargoTipo.toDF()
Join10 = Join09.join(cago_tipo_df,
        (Join09["cargo_car_tipo_id"] == cago_tipo_df["CargoTipo_car_tipo_id"]),
        how="left")
# print("==========Join10==========")
# Join10.printSchema()
# select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("cargo_car_tipo_id"),col("CargoTipo_car_tipo_nombre"),col("CargoTipo_car_tipo_id")).show(10)
# print("==========================")

# ========== Join11 (join10 vs Sociedad) ===========
# ==================================================
Sociedad_df = Sociedad.toDF()
Join11 = Join10.join(Sociedad_df,
        (Join10["empleado_sociedad_id"] == Sociedad_df["Sociedad_sociedad_id"]),
        how="left")
# print("==========Join11==========")
# Join11.printSchema()
# Join11.select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("Sociedad_sociedad_id"),col("Sociedad_sociedad_nombre")).show(10)
# print("==========================")

# ========== Join12 (join11 vs EmpleadoUbicacion) ===============
# ==================================================
EmpleadoUbicacion_df = EmpleadoUbicacion.toDF()
EmpleadoUbicacion_df = EmpleadoUbicacion_df.withColumn('tmp_empleado_ubicacion_fechahasta',
                                                        when(EmpleadoUbicacion_df["EmpleadoUbicacion_fechahasta"].isNull(), datetime.now()).otherwise(EmpleadoUbicacion_df["EmpleadoUbicacion_fechahasta"]))
                                                                        
Join12_df = Join11.join(EmpleadoUbicacion_df,
        (Join11["empleado_emp_id"] == EmpleadoUbicacion_df["EmpleadoUbicacion_emp_id"]) &
        (Join11.calendario_fecha >= EmpleadoUbicacion_df.EmpleadoUbicacion_fechadesde) &
        (Join11.calendario_fecha <= EmpleadoUbicacion_df.tmp_empleado_ubicacion_fechahasta),
        how="left")
Join12 = Join12_df.drop("tmp_empleado_ubicacion_fechahasta")
# print("==========Join12==========")
# Join12.printSchema()
# Join12.select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("EmpleadoUbicacion_emp_id"),col("EmpleadoUbicacion_fechadesde")).show(10)
# print("==========================")

# ========== Join13 (join12 vs Site) ===============
# ==================================================
Site_df = Site.toDF()
Join13 = Join12.join(Site_df,
        (Join12["EmpleadoUbicacion_site_id"] == Site_df["Site_site_id"]),
        how="left")
# print("==========Join13==========")
# Join13.printSchema()
# Join13.select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("Site_site_id"),col("Site_site_nombre")).show(10)
# print("==========================")

maestro = Join13.select(
    col("calendario_fecha"),
    col("empleado_emp_id"),
    col("empleado_emp_docnro"),
    col("empleado_emp_apellido"),
    col("empleado_emp_nombre"),
    col("empleado_suc_id"),
    col("cargo_car_id"),
    col("cargo_car_nombre"),
    col("CargoTipo_car_tipo_id"),
    col("CargoTipo_car_tipo_nombre"),
    col("cuenta_cue_id"),
    col("cuenta_cue_nombre"),
    col("sub_area_sar_id"),
    col("sub_area_sar_nombre"),
    col("empleado_cuenta_ecu_fechadesde"),
    col("empleado_cuenta_ecu_fechahasta"),
    col("servicios_srv_id"),
    col("servicios_srv_nombre"),
    col("empleado_servicio_empsrv_fechadesde"),
    col("empleado_servicio_empsrv_fechahasta"),
    col("Sociedad_sociedad_nombre"),
    col("Site_site_nombre"),
    col("empleado_anulado"),
    col("modalidad_trabajo_id_mod_trab"),
    col("modalidad_trabajo_modalidad"),
    ).distinct()

# | JOINS NIVEL 2|

# ========== Maestro Superiores: Join_sup_1 ====================
# ==============================================================
EmpleadoSuperiores_df = EmpleadoSuperiores.toDF()
EmpleadoSuperiores_df = EmpleadoSuperiores_df.withColumn('tmp_empleado_superiores_fechahasta',
                                                                        when(EmpleadoSuperiores_df["empleado_superiores_fechahasta"].isNull(), 
                                                                        to_utc_timestamp(current_timestamp(),'GMT+5')).otherwise
                                                                        (EmpleadoSuperiores_df["empleado_superiores_fechahasta"])).select(
                                                                            col("empleado_superiores_emp_id").alias("sup_1_emp_id"),
                                                                            col("empleado_superiores_fechadesde").alias("sup_1_fechadesde"),
                                                                            col("empleado_superiores_fechahasta").alias("sup_1_fechahasta"),
                                                                            col("tmp_empleado_superiores_fechahasta").alias("tmp_sup_1_fechahasta"),
                                                                            col("empleado_superiores_emp_idsup").alias("sup_1_emp_idsup"))

Join_sup_1 = maestro.join(EmpleadoSuperiores_df,
                (maestro["empleado_emp_id"] == EmpleadoSuperiores_df["sup_1_emp_id"]) &
                (maestro.calendario_fecha >= EmpleadoSuperiores_df.sup_1_fechadesde) &
                (maestro.calendario_fecha <= EmpleadoSuperiores_df.tmp_sup_1_fechahasta),
                how="left")
Join_sup_1 = Join_sup_1.drop("tmp_sup_1_fechahasta")
# print("==========Join_sup_1==========")
# Join_sup_1.printSchema()
# Join_sup_1.select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_id"),col("sup_1_id_empsup")).show(10, truncate=False)
# maestro.filter(maestro.empleado_emp_id == 18021).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id")).show(40, truncate=False)
# EmpleadoSuperiores_df.filter(EmpleadoSuperiores_df.sup_1_emp_id == 18021).show(40, truncate=False)
# print("==========================")

# ========== Maestro Superiores: Join_sup_2 ====================
# ==============================================================
EmpleadoSuperiores_df = EmpleadoSuperiores.toDF()
EmpleadoSuperiores_df = EmpleadoSuperiores_df.withColumn('tmp_empleado_superiores_fechahasta',
                                                                        when(EmpleadoSuperiores_df["empleado_superiores_fechahasta"].isNull(), 
                                                                        to_utc_timestamp(current_timestamp(),'GMT+5')).otherwise
                                                                        (EmpleadoSuperiores_df["empleado_superiores_fechahasta"])).select(
                                                                            col("empleado_superiores_emp_id").alias("sup_2_emp_id"),
                                                                            col("empleado_superiores_fechadesde").alias("sup_2_fechadesde"),
                                                                            col("empleado_superiores_fechahasta").alias("sup_2_fechahasta"),
                                                                            col("tmp_empleado_superiores_fechahasta").alias("tmp_sup_2_fechahasta"),
                                                                            col("empleado_superiores_emp_idsup").alias("sup_2_emp_idsup"))
                                                                            
Join_sup_2 = Join_sup_1.join(EmpleadoSuperiores_df,
                (Join_sup_1["sup_1_emp_idsup"] == EmpleadoSuperiores_df["sup_2_emp_id"]) &
                (Join_sup_1.calendario_fecha >= EmpleadoSuperiores_df.sup_2_fechadesde) &
                (Join_sup_1.calendario_fecha <= EmpleadoSuperiores_df.tmp_sup_2_fechahasta),
                how="left")
Join_sup_2 = Join_sup_2.drop("tmp_sup_2_fechahasta")
# print("==========Join_sup_2==========")
# Join_sup_2.printSchema()
# Join_sup_2.filter(Join_sup_2.empleado_emp_id == 18021).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_idsup"),col("sup_2_emp_idsup")).show(40, truncate=False)
# print("================================")

# ========== Maestro Superiores: Join_sup_3 ====================
# ==============================================================
EmpleadoSuperiores_df = EmpleadoSuperiores.toDF()
EmpleadoSuperiores_df = EmpleadoSuperiores_df.withColumn('tmp_empleado_superiores_fechahasta',
                                                                        when(EmpleadoSuperiores_df["empleado_superiores_fechahasta"].isNull(), 
                                                                        to_utc_timestamp(current_timestamp(),'GMT+5')).otherwise
                                                                        (EmpleadoSuperiores_df["empleado_superiores_fechahasta"])).select(
                                                                            col("empleado_superiores_emp_id").alias("sup_3_emp_id"),
                                                                            col("empleado_superiores_fechadesde").alias("sup_3_fechadesde"),
                                                                            col("empleado_superiores_fechahasta").alias("sup_3_fechahasta"),
                                                                            col("tmp_empleado_superiores_fechahasta").alias("tmp_sup_3_fechahasta"),
                                                                            col("empleado_superiores_emp_idsup").alias("sup_3_emp_idsup"))
                                                                            
Join_sup_3 = Join_sup_2.join(EmpleadoSuperiores_df,
                (Join_sup_2["sup_2_emp_idsup"] == EmpleadoSuperiores_df["sup_3_emp_id"]) &
                (Join_sup_2.calendario_fecha >= EmpleadoSuperiores_df.sup_3_fechadesde) &
                (Join_sup_2.calendario_fecha <= EmpleadoSuperiores_df.tmp_sup_3_fechahasta),
                how="left")
Join_sup_3 = Join_sup_3.drop("tmp_sup_3_fechahasta")
# print("==========Join_sup_3==========")
# Join_sup_3.printSchema()
# Join_sup_3.filter(Join_sup_3.empleado_emp_id == 18021).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_idsup"),col("sup_2_emp_idsup"),
# col("sup_3_emp_idsup")).show(40, truncate=False)
# print("================================")

# ========== Maestro Superiores: Join_sup_4 ====================
# ==============================================================
EmpleadoSuperiores_df = EmpleadoSuperiores.toDF()
EmpleadoSuperiores_df = EmpleadoSuperiores_df.withColumn('tmp_empleado_superiores_fechahasta',
                                                                        when(EmpleadoSuperiores_df["empleado_superiores_fechahasta"].isNull(), 
                                                                        to_utc_timestamp(current_timestamp(),'GMT+5')).otherwise
                                                                        (EmpleadoSuperiores_df["empleado_superiores_fechahasta"])).select(
                                                                            col("empleado_superiores_emp_id").alias("sup_4_emp_id"),
                                                                            col("empleado_superiores_fechadesde").alias("sup_4_fechadesde"),
                                                                            col("empleado_superiores_fechahasta").alias("sup_4_fechahasta"),
                                                                            col("tmp_empleado_superiores_fechahasta").alias("tmp_sup_4_fechahasta"),
                                                                            col("empleado_superiores_emp_idsup").alias("sup_4_emp_idsup"))
                                                                            
Join_sup_4 = Join_sup_3.join(EmpleadoSuperiores_df,
                (Join_sup_3["sup_3_emp_idsup"] == EmpleadoSuperiores_df["sup_4_emp_id"]) &
                (Join_sup_3.calendario_fecha >= EmpleadoSuperiores_df.sup_4_fechadesde) &
                (Join_sup_3.calendario_fecha <= EmpleadoSuperiores_df.tmp_sup_4_fechahasta),
                how="left")
Join_sup_4 = Join_sup_4.drop("tmp_sup_4_fechahasta")
# print("==========Join_sup_4==========")
# Join_sup_4.printSchema()
# Join_sup_4.filter(Join_sup_4.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_idsup"),col("sup_2_emp_idsup"),
# col("sup_3_emp_idsup"), col("sup_4_emp_idsup")).show(40, truncate=False)
# print("================================")

# ========== Maestro Superiores: Join_sup_5 ====================
# ==============================================================
EmpleadoSuperiores_df = EmpleadoSuperiores.toDF()
EmpleadoSuperiores_df = EmpleadoSuperiores_df.withColumn('tmp_empleado_superiores_fechahasta',
                                                                        when(EmpleadoSuperiores_df["empleado_superiores_fechahasta"].isNull(), 
                                                                        to_utc_timestamp(current_timestamp(),'GMT+5')).otherwise
                                                                        (EmpleadoSuperiores_df["empleado_superiores_fechahasta"])).select(
                                                                            col("empleado_superiores_emp_id").alias("sup_5_emp_id"),
                                                                            col("empleado_superiores_fechadesde").alias("sup_5_fechadesde"),
                                                                            col("empleado_superiores_fechahasta").alias("sup_5_fechahasta"),
                                                                            col("tmp_empleado_superiores_fechahasta").alias("tmp_sup_5_fechahasta"),
                                                                            col("empleado_superiores_emp_idsup").alias("sup_5_emp_idsup"))
                                                                            
Join_sup_5 = Join_sup_4.join(EmpleadoSuperiores_df,
                (Join_sup_4["sup_4_emp_idsup"] == EmpleadoSuperiores_df["sup_5_emp_id"]) &
                (Join_sup_4.calendario_fecha >= EmpleadoSuperiores_df.sup_5_fechadesde) &
                (Join_sup_4.calendario_fecha <= EmpleadoSuperiores_df.tmp_sup_5_fechahasta),
                how="left")
Join_sup_5 = Join_sup_5.drop("tmp_sup_5_fechahasta")
# print("==========Join_sup_5==========")
# Join_sup_5.printSchema()
# Join_sup_5.filter(Join_sup_5.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_idsup"),col("sup_2_emp_idsup"),
# col("sup_3_emp_idsup"), col("sup_4_emp_idsup"), col("sup_5_emp_idsup")).show(40, truncate=False)
# print("================================")

# ========== Maestro Superiores: Join_sup_6 ====================
# ==============================================================
EmpleadoSuperiores_df = EmpleadoSuperiores.toDF()
EmpleadoSuperiores_df = EmpleadoSuperiores_df.withColumn('tmp_empleado_superiores_fechahasta',
                                                                        when(EmpleadoSuperiores_df["empleado_superiores_fechahasta"].isNull(), 
                                                                        to_utc_timestamp(current_timestamp(),'GMT+5')).otherwise
                                                                        (EmpleadoSuperiores_df["empleado_superiores_fechahasta"])).select(
                                                                            col("empleado_superiores_emp_id").alias("sup_6_emp_id"),
                                                                            col("empleado_superiores_fechadesde").alias("sup_6_fechadesde"),
                                                                            col("empleado_superiores_fechahasta").alias("sup_6_fechahasta"),
                                                                            col("tmp_empleado_superiores_fechahasta").alias("tmp_sup_6_fechahasta"),
                                                                            col("empleado_superiores_emp_idsup").alias("sup_6_emp_idsup"))
                                                                            
Join_sup_6 = Join_sup_5.join(EmpleadoSuperiores_df,
                (Join_sup_5["sup_5_emp_idsup"] == EmpleadoSuperiores_df["sup_6_emp_id"]) &
                (Join_sup_5.calendario_fecha >= EmpleadoSuperiores_df.sup_6_fechadesde) &
                (Join_sup_5.calendario_fecha <= EmpleadoSuperiores_df.tmp_sup_6_fechahasta),
                how="left")
Join_sup_6 = Join_sup_6.drop("tmp_sup_6_fechahasta")
# print("==========Join_sup_6==========")
# Join_sup_6.printSchema()
# Join_sup_6.filter(Join_sup_6.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_idsup"),col("sup_2_emp_idsup"),
# col("sup_3_emp_idsup"), col("sup_4_emp_idsup"), col("sup_5_emp_idsup"), col("sup_6_emp_idsup")).show(40, truncate=False)
# print("================================")

# | JOINS NIVEL 3|

# ========== Maestro Superiores: m_1 ====================
# ==================================================
maestro_superiores = Join_sup_6
m_1 = Join_sup_6.select(col("empleado_emp_id").alias("m1_emp_id"),
                        col("calendario_fecha").alias("m1_fecha"),
                        col("CargoTipo_car_tipo_id").alias("m1_car_tipo_id"),
                        col("CargoTipo_car_tipo_nombre").alias("m1_car_tipo_nombre"), 
                        concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("m1_empleado"))

Join_m1 = maestro_superiores.join(m_1,
            (maestro_superiores.sup_1_emp_idsup == m_1.m1_emp_id) &
            (maestro_superiores.calendario_fecha == m_1.m1_fecha),
            how = "left")
# print("==========Join_sup_6==========")
# Join_m1.printSchema()
# Join_m1.show(10)
# Join_m1.filter(Join_m1.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("sup_1_emp_idsup"),col("sup_2_emp_idsup"),
# col("sup_3_emp_idsup"), col("sup_4_emp_idsup"), col("sup_5_emp_idsup"), col("sup_6_emp_idsup"), col("m1_car_tipo_id"),col("m1_car_tipo_nombre"),
# col("m1_emp_id"), col("m1_empleado")).show(40, truncate=False)
# m_1.filter(m_1.m1_emp_id == 67363).show(40, truncate=False)
# print("================================")

# ========== Maestro Superiores: m_2 ====================
# =======================================================
m_2 = Join_sup_6.select(col("empleado_emp_id").alias("m2_emp_id"),
                        col("calendario_fecha").alias("m2_fecha"),
                        col("CargoTipo_car_tipo_id").alias("m2_car_tipo_id"),
                        col("CargoTipo_car_tipo_nombre").alias("m2_car_tipo_nombre"), 
                        concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("m2_empleado"))

Join_m2 = Join_m1.join(m_2,
            (Join_m1.sup_2_emp_idsup == m_2.m2_emp_id) &
            (Join_m1.calendario_fecha == m_2.m2_fecha),
            how = "left")
# print("==========Join_m2===============")
# Join_m2.printSchema()
# Join_m2.show(10)
# Join_m2.filter(Join_m2.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("m2_car_tipo_id"),col("m2_car_tipo_nombre"),
# col("m2_emp_id"), col("m2_empleado")).show(40, truncate=False)
# print("================================")

# ========== Maestro Superiores: m_3 ====================
# =======================================================
m_3 = Join_sup_6.select(col("empleado_emp_id").alias("m3_emp_id"),
                        col("calendario_fecha").alias("m3_fecha"),
                        col("CargoTipo_car_tipo_id").alias("m3_car_tipo_id"),
                        col("CargoTipo_car_tipo_nombre").alias("m3_car_tipo_nombre"), 
                        concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("m3_empleado"))

Join_m3 = Join_m2.join(m_3,
            (Join_m2.sup_3_emp_idsup == m_3.m3_emp_id) &
            (Join_m2.calendario_fecha == m_3.m3_fecha),
            how = "left")
# print("==========Join_m3===============")
# Join_m3.printSchema()
# Join_m3.show(10)
# Join_m3.filter(Join_m3.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("m3_car_tipo_id"),col("m3_car_tipo_nombre"),
# col("m3_emp_id"), col("m3_empleado")).orderBy(col("calendario_fecha").asc()).show(10, truncate=False)
# print("================================")

# ========== Maestro Superiores: m_4 ====================
# =======================================================
m_4 = Join_sup_6.select(col("empleado_emp_id").alias("m4_emp_id"),
                        col("calendario_fecha").alias("m4_fecha"),
                        col("CargoTipo_car_tipo_id").alias("m4_car_tipo_id"),
                        col("CargoTipo_car_tipo_nombre").alias("m4_car_tipo_nombre"), 
                        concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("m4_empleado"))

Join_m4 = Join_m3.join(m_4,
            (Join_m3.sup_4_emp_idsup == m_4.m4_emp_id) &
            (Join_m3.calendario_fecha == m_4.m4_fecha),
            how = "left")
# print("==========Join_m4===============")
# Join_m4.printSchema()
# Join_m4.show(10)
# Join_m4.filter(Join_m4.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("m4_car_tipo_id"),col("m4_car_tipo_nombre"),
# col("m4_emp_id"), col("m4_empleado")).orderBy(col("calendario_fecha").asc()).show(10, truncate=False)
# print("================================")

# ========== Maestro Superiores: m_5 ====================
# =======================================================
m_5 = Join_sup_6.select(col("empleado_emp_id").alias("m5_emp_id"),
                        col("calendario_fecha").alias("m5_fecha"),
                        col("CargoTipo_car_tipo_id").alias("m5_car_tipo_id"),
                        col("CargoTipo_car_tipo_nombre").alias("m5_car_tipo_nombre"), 
                        concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("m5_empleado"))

Join_m5 = Join_m4.join(m_5,
            (Join_m4.sup_5_emp_idsup == m_5.m5_emp_id) &
            (Join_m4.calendario_fecha == m_5.m5_fecha),
            how = "left")
# print("==========Join_m5===============")
# Join_m5.printSchema()
# Join_m5.show(10)
# Join_m5.filter(Join_m5.empleado_emp_id == 148457).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("m5_car_tipo_id"),col("m5_car_tipo_nombre"),
# col("m5_emp_id"), col("m5_empleado")).orderBy(col("calendario_fecha").asc()).show(10, truncate=False)
# print("================================")

# ========== Maestro Superiores: m_6 ====================
# =======================================================
m_6 = Join_sup_6.select(col("empleado_emp_id").alias("m6_emp_id"),
                        col("calendario_fecha").alias("m6_fecha"),
                        col("CargoTipo_car_tipo_id").alias("m6_car_tipo_id"),
                        col("CargoTipo_car_tipo_nombre").alias("m6_car_tipo_nombre"), 
                        concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("m6_empleado"))

Join_m6 = Join_m5.join(m_6,
            (Join_m5.sup_6_emp_idsup == m_6.m6_emp_id) &
            (Join_m5.calendario_fecha == m_6.m6_fecha),
            how = "left")
# print("==========Join_m6===============")
# Join_m6.printSchema()
# Join_m6.show(10)
# Join_m6.filter(Join_m6.empleado_emp_id == 18209).select(col("calendario_fecha"),col("empleado_cuenta_cue_id"),col("empleado_emp_id"),col("m6_car_tipo_id"),col("m6_car_tipo_nombre"),
# col("m6_emp_id"), col("m6_empleado")).orderBy(col("calendario_fecha").asc()).show(10, truncate=False)
# print("================================")

# ========== Maestro Superiores Orden ====================
# ========================================================
maestro_superiores_orden = Join_m6

maestro_final = maestro_superiores_orden.withColumn("lider_id",
                                     when(col("m1_car_tipo_id") == 5, col("m1_emp_id"))
                                    .when(col("m2_car_tipo_id") == 5, col("m2_emp_id"))
                                    .when(col("m3_car_tipo_id") == 5, col("m3_emp_id"))
                                    .when(col("m4_car_tipo_id") == 5, col("m4_emp_id"))
                                    .when(col("m5_car_tipo_id") == 5, col("m5_emp_id"))
                                    .when(col("m6_car_tipo_id") == 5, col("m6_emp_id"))
                                    ).withColumn("lider",
                                     when(col("m1_car_tipo_id") == 5, col("m1_empleado"))
                                    .when(col("m2_car_tipo_id") == 5, col("m2_empleado"))
                                    .when(col("m3_car_tipo_id") == 5, col("m3_empleado"))
                                    .when(col("m4_car_tipo_id") == 5, col("m4_empleado"))
                                    .when(col("m5_car_tipo_id") == 5, col("m5_empleado"))
                                    .when(col("m6_car_tipo_id") == 5, col("m6_empleado"))
                                    ).withColumn("coordinador_id",
                                     when(col("m1_car_tipo_id") == 22, col("m1_emp_id"))
                                    .when(col("m2_car_tipo_id") == 22, col("m2_emp_id"))
                                    .when(col("m3_car_tipo_id") == 22, col("m3_emp_id"))
                                    .when(col("m4_car_tipo_id") == 22, col("m4_emp_id"))
                                    .when(col("m5_car_tipo_id") == 22, col("m5_emp_id"))
                                    .when(col("m6_car_tipo_id") == 22, col("m6_emp_id"))
                                    ).withColumn("coordinador",
                                     when(col("m1_car_tipo_id") == 22, col("m1_empleado"))
                                    .when(col("m2_car_tipo_id") == 22, col("m2_empleado"))
                                    .when(col("m3_car_tipo_id") == 22, col("m3_empleado"))
                                    .when(col("m4_car_tipo_id") == 22, col("m4_empleado"))
                                    .when(col("m5_car_tipo_id") == 22, col("m5_empleado"))
                                    .when(col("m6_car_tipo_id") == 22, col("m6_empleado"))
                                    ).withColumn("responsable_id",
                                     when(col("m1_car_tipo_id") == 6, col("m1_emp_id"))
                                    .when(col("m2_car_tipo_id") == 6, col("m2_emp_id"))
                                    .when(col("m3_car_tipo_id") == 6, col("m3_emp_id"))
                                    .when(col("m4_car_tipo_id") == 6, col("m4_emp_id"))
                                    .when(col("m5_car_tipo_id") == 6, col("m5_emp_id"))
                                    .when(col("m6_car_tipo_id") == 6, col("m6_emp_id"))
                                    ).withColumn("responsable",
                                     when(col("m1_car_tipo_id") == 6, col("m1_empleado"))
                                    .when(col("m2_car_tipo_id") == 6, col("m2_empleado"))
                                    .when(col("m3_car_tipo_id") == 6, col("m3_empleado"))
                                    .when(col("m4_car_tipo_id") == 6, col("m4_empleado"))
                                    .when(col("m5_car_tipo_id") == 6, col("m5_empleado"))
                                    .when(col("m6_car_tipo_id") == 6, col("m6_empleado"))
                                    ).withColumn("jefe_id",
                                     when(col("m1_car_tipo_id") == 4, col("m1_emp_id"))
                                    .when(col("m2_car_tipo_id") == 4, col("m2_emp_id"))
                                    .when(col("m3_car_tipo_id") == 4, col("m3_emp_id"))
                                    .when(col("m4_car_tipo_id") == 4, col("m4_emp_id"))
                                    .when(col("m5_car_tipo_id") == 4, col("m5_emp_id"))
                                    .when(col("m6_car_tipo_id") == 4, col("m6_emp_id"))
                                    ).withColumn("jefe",
                                     when(col("m1_car_tipo_id") == 4, col("m1_empleado"))
                                    .when(col("m2_car_tipo_id") == 4, col("m2_empleado"))
                                    .when(col("m3_car_tipo_id") == 4, col("m3_empleado"))
                                    .when(col("m4_car_tipo_id") == 4, col("m4_empleado"))
                                    .when(col("m5_car_tipo_id") == 4, col("m5_empleado"))
                                    .when(col("m6_car_tipo_id") == 4, col("m6_empleado"))
                                    ).withColumn("gerente_id",
                                     when(col("m1_car_tipo_id") == 3, col("m1_emp_id"))
                                    .when(col("m2_car_tipo_id") == 3, col("m2_emp_id"))
                                    .when(col("m3_car_tipo_id") == 3, col("m3_emp_id"))
                                    .when(col("m4_car_tipo_id") == 3, col("m4_emp_id"))
                                    .when(col("m5_car_tipo_id") == 3, col("m5_emp_id"))
                                    .when(col("m6_car_tipo_id") == 3, col("m6_emp_id"))
                                    ).withColumn("gerente",
                                     when(col("m1_car_tipo_id") == 3, col("m1_empleado"))
                                    .when(col("m2_car_tipo_id") == 3, col("m2_empleado"))
                                    .when(col("m3_car_tipo_id") == 3, col("m3_empleado"))
                                    .when(col("m4_car_tipo_id") == 3, col("m4_empleado"))
                                    .when(col("m5_car_tipo_id") == 3, col("m5_empleado"))
                                    .when(col("m6_car_tipo_id") == 3, col("m6_empleado"))
                                    ).withColumn("director_id",
                                     when(col("m1_car_tipo_id") == 2, col("m1_emp_id"))
                                    .when(col("m2_car_tipo_id") == 2, col("m2_emp_id"))
                                    .when(col("m3_car_tipo_id") == 2, col("m3_emp_id"))
                                    .when(col("m4_car_tipo_id") == 2, col("m4_emp_id"))
                                    .when(col("m5_car_tipo_id") == 2, col("m5_emp_id"))
                                    .when(col("m6_car_tipo_id") == 2, col("m6_emp_id"))
                                    ).withColumn("director",
                                     when(col("m1_car_tipo_id") == 2, col("m1_empleado"))
                                    .when(col("m2_car_tipo_id") == 2, col("m2_empleado"))
                                    .when(col("m3_car_tipo_id") == 2, col("m3_empleado"))
                                    .when(col("m4_car_tipo_id") == 2, col("m4_empleado"))
                                    .when(col("m5_car_tipo_id") == 2, col("m5_empleado"))
                                    .when(col("m6_car_tipo_id") == 2, col("m6_empleado"))
                                    )

# print("==========maestro_final===============")
# maestro_final.printSchema()
# maestro_final.show(10)
# maestro_final.filter(maestro_final.empleado_emp_id == 148457).select(col("empleado_emp_id"),col("lider_id"), col("lider"), col("coordinador_id"), col("coordinador")
# , col("responsable_id"), col("responsable"), col("jefe_id"), col("jefe"), col("gerente_id"), col("gerente")).orderBy(col("calendario_fecha").asc()).show(10, truncate=False)
# print("================================")

maestro_carga = maestro_final.select(
    col("calendario_fecha").alias("fecha"),
    col("empleado_emp_id").alias("meucci_id"),
    col("empleado_emp_docnro").alias("documento"),
    concat_ws(", ","empleado_emp_apellido","empleado_emp_nombre").alias("empleado"),
    col("empleado_suc_id").alias("sucursal_id"),
    col("lider_id"),
    col("lider"),
    col("coordinador_id"),
    col("coordinador"),
    col("responsable_id"),
    col("responsable"),
    col("jefe_id"),
    col("jefe"),
    col("gerente_id"),
    col("gerente"),
    col("cuenta_cue_id").alias("cuenta_id"),
    col("cuenta_cue_nombre").alias("cuenta"),
    col("sub_area_sar_id").alias("subarea_id"),
    col("sub_area_sar_nombre").alias("subarea"),
    col("cargo_car_id").alias("cargo_id"),
    col("cargo_car_nombre").alias("cargo"),
    col("empleado_cuenta_ecu_fechadesde").alias("fecha_inicio"),
    col("empleado_cuenta_ecu_fechahasta").alias("fecha_fin"),
    col("servicios_srv_id").alias("servicio_id"),
    col("servicios_srv_nombre").alias("servicio"),
    col("empleado_servicio_empsrv_fechadesde").alias("fecha_inicio_servicio"),
    col("empleado_servicio_empsrv_fechahasta").alias("fecha_fin_servicio"),
    col("empleado_anulado").alias("anulado"),
    col("modalidad_trabajo_id_mod_trab").alias("modalidad_id"),
    col("modalidad_trabajo_modalidad").alias("modalidad"),
    col("Site_site_nombre").alias("site_nombre"),
    col("Sociedad_sociedad_nombre").alias("sociedad"),
    col("director_id"),
    col("director"),
    col("CargoTipo_car_tipo_id").alias("tipo_cargo_id"),
    col("CargoTipo_car_tipo_nombre").alias("tipo_cargo"),
    )

maestro_resultado = maestro_carga.withColumn(
    "lider_id", when(maestro_carga.lider_id.isNull(),0
    ).otherwise(maestro_carga.lider_id)).withColumn(
        "lider", when(maestro_carga.lider.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.lider)).withColumn(
        "coordinador_id", when(maestro_carga.coordinador_id.isNull(),0
    ).otherwise(maestro_carga.coordinador_id)).withColumn(
        "coordinador", when(maestro_carga.coordinador.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.coordinador)).withColumn(
        "responsable_id", when(maestro_carga.responsable_id.isNull(),0
    ).otherwise(maestro_carga.responsable_id)).withColumn(
        "responsable", when(maestro_carga.responsable.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.responsable)).withColumn(
        "jefe_id", when(maestro_carga.jefe_id.isNull(),0
    ).otherwise(maestro_carga.jefe_id)).withColumn(
        "jefe", when(maestro_carga.jefe.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.jefe)).withColumn(
        "gerente_id", when(maestro_carga.gerente_id.isNull(),0
    ).otherwise(maestro_carga.gerente_id)).withColumn(
        "gerente", when(maestro_carga.gerente.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.gerente)).withColumn(
        "director_id", when(maestro_carga.director_id.isNull(),0
    ).otherwise(maestro_carga.director_id)).withColumn(
        "director", when(maestro_carga.director.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.director)).withColumn(
        "tipo_cargo_id", when(maestro_carga.tipo_cargo_id.isNull(),0
    ).otherwise(maestro_carga.tipo_cargo_id)).withColumn(
        "tipo_cargo", when(maestro_carga.tipo_cargo.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.tipo_cargo)).withColumn(
        "srv_id", when(maestro_carga.servicio_id.isNull(),0
    ).otherwise(maestro_carga.servicio_id)).withColumn(
        "srv_nombre", when(maestro_carga.servicio.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.servicio)).withColumn(
        "cue_id", when(maestro_carga.cuenta_id.isNull(),0
    ).otherwise(maestro_carga.cuenta_id)).withColumn(
        "cue_nombre", when(maestro_carga.cuenta.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.cuenta)).withColumn(
        "sar_id", when(maestro_carga.subarea_id.isNull(),0
    ).otherwise(maestro_carga.subarea_id)).withColumn(
        "sar_nombre", when(maestro_carga.subarea.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.subarea)).withColumn(
        "mod_id", when(maestro_carga.modalidad_id.isNull(),0
    ).otherwise(maestro_carga.modalidad_id)).withColumn(
        "mod_nombre", when(maestro_carga.modalidad.isNull(),'<<SIN REGISTRO>>'
    ).otherwise(maestro_carga.modalidad))

ResultSrc = DynamicFrame.fromDF(
    maestro_resultado,
    glueContext,
    "ResultSrc",
)

Result = ApplyMapping.apply(
    frame=ResultSrc,
    mappings=[
        ("fecha", "timestamp", "fecha", "date"),
        ("meucci_id", "int", "meucci_id", "int"),
        ("documento", "int", "documento", "int"),
        ("empleado", "string", "empleado", "string"),
        ("sucursal_id", "int", "sucursal_id", "int"),
        ("lider_id", "int", "lider_id", "int"),
        ("lider", "string", "lider", "string"),
        ("coordinador_id", "int", "coordinador_id", "int"),
        ("coordinador", "string", "coordinador", "string"),
        ("responsable_id", "int", "responsable_id", "int"),
        ("responsable", "string", "responsable", "string"),
        ("jefe_id", "int", "jefe_id", "int"),
        ("jefe", "string", "jefe", "string"),
        ("gerente_id", "int", "gerente_id", "int"),
        ("gerente", "string", "gerente", "string"),
        ("cue_id", "int", "cuenta_id", "int"),
        ("cue_nombre", "string", "cuenta", "string"),
        ("sar_id", "int", "subarea_id", "int"),
        ("sar_nombre", "string", "subarea", "string"),
        ("cargo_id", "int", "cargo_id", "int"),
        ("cargo", "string", "cargo", "string"),
        ("fecha_inicio", "timestamp", "fecha_inicio", "date"),
        ("fecha_fin", "timestamp", "fecha_fin", "date"),
        ("srv_id", "int", "servicio_id", "int"),
        ("srv_nombre", "string", "servicio", "string"),
        ("fecha_inicio_servicio", "timestamp", "fecha_inicio_servicio", "date"),
        ("fecha_fin_servicio", "timestamp", "fecha_fin_servicio", "date"),
        ("anulado", "byte", "anulado", "byte"),
        ("mod_id", "int", "modalidad_id", "int"),
        ("mod_nombre", "string", "modalidad", "string"),
        ("site_nombre", "string", "site_nombre", "string"),
        ("sociedad", "string", "sociedad", "string"),
        ("director_id", "int", "director_id", "int"),
        ("director", "string", "director", "string"),
        ("tipo_cargo_id", "int", "tipo_cargo_id", "int"),
        ("tipo_cargo", "string", "tipo_cargo", "string"),
    ],
    transformation_ctx="Result",
)

# ==================
# BORRAR PARTICIONES
# ==================
FECHA_INI_DATE = datetime.now(TIMEZONE).date() - timedelta(days=TARGET_DAYS)
FECHA_FIN_DATE = datetime.now(TIMEZONE).date()
try:
    # Verifica la existencia de la tabla
    response = glue_client.get_table(
        DatabaseName=TARGET_DATABASE,
        Name=TARGET_TABLE_MAESTRO_EQUIPOS
    )
    print(f'La tabla {TARGET_DATABASE}.{TARGET_TABLE_MAESTRO_EQUIPOS} existe en el catálogo de Glue.')
    validate = 1
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        print(f'La tabla {TARGET_DATABASE}.{TARGET_TABLE_MAESTRO_EQUIPOS} no existe en el catálogo de Glue.')
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
        print(f"Get partition from {TARGET_DATABASE}.{TARGET_TABLE_MAESTRO_EQUIPOS} where {_expression}")
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
                                              table=TARGET_TABLE_MAESTRO_EQUIPOS,
                                              _from=FECHA_INI_DATE,
                                              _to=FECHA_FIN_DATE)

# ==================
# CARGAR PARTICIONES
# ==================
target_maestro_equipos_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Result,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": TARGET_BUCKET,
                        "partitionKeys": ["fecha"]},
    format_options={"compression": "snappy"},
    transformation_ctx="target_maestro_equipos_node3",
)

job.commit()
