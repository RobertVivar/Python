from .etl import DML
from .etl_mail import Mail
from pandas import DataFrame
from .function.get_column import get_column_map
from os import path
import pandas as pd

class TransformData(DML):

    def rename_column(self, data: DataFrame) -> DataFrame:
        try:
            column_map = get_column_map(id_columns=self.id_column)
            columns = [column_map[value] for value in column_map]
            data_rename = data.rename(columns=column_map)
            data_transform = data_rename[columns]

            return data_transform

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_cuenta(self, data: DataFrame) -> DataFrame:
        try:
            data["rel_id"] = data["rel_id"].astype("Int64")

            return data
        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleado(self, data: DataFrame) -> DataFrame:
        try:
            data["emp_legbejerman"] = data["emp_legbejerman"].astype("Int64")
            data["emp_docnro"] = data["emp_docnro"].astype("Int64")
            data["eci_id"] = data["eci_id"].astype("Int64")
            data["bar_id"] = data["bar_id"].astype("Int64")
            data["loc_id"] = data["loc_id"].astype("Int64")
            data["prov_id"] = data["prov_id"].astype("Int64")
            data["pai_id"] = data["pai_id"].astype("Int64")
            data["oso_id"] = data["oso_id"].astype("Int64")
            data["ubi_id"] = data["ubi_id"].astype("Int64")
            data["suc_id"] = data["suc_id"].astype("Int64")
            data["loc_id_nac"] = data["loc_id_nac"].astype("Int64")
            data["Pro_id_nac"] = data["Pro_id_nac"].astype("Int64")
            data["pais_id_nac"] = data["pais_id_nac"].astype("Int64")
            data["suc_id_cobra"] = data["suc_id_cobra"].astype("Int64")
            data["curr_id"] = data["curr_id"].astype("Int64")
            data["gsa_id"] = data["gsa_id"].astype("Int64")
            data["sociedad_id"] = data["sociedad_id"].astype("Int64")
            data["idpersonalsap"] = data["idpersonalsap"].astype("Int64")
            data["emp_canthijos"] = data["emp_canthijos"].astype("Int64")
            data["cnt_fijo"] = data["cnt_fijo"].astype("Int64")

            return data
        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleado_cuenta(self, data: DataFrame) -> DataFrame:
        try:
            data["ecu_id"] = data["ecu_id"].astype("Int64")
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["cue_id"] = data["cue_id"].astype("Int64")
            data["car_id"] = data["car_id"].astype("Int64")
            data["ecu_fechadesde"] = pd.to_datetime(data['ecu_fechadesde'], errors='coerce')
            data["ecu_fechahasta"] = pd.to_datetime(data['ecu_fechahasta'], errors='coerce')
            data["ecu_tanda"] = data["ecu_tanda"].astype("Int64")
            data["anulado"] = data["anulado"].astype("Int64")
            data["yaactualizado"] = data["yaactualizado"].astype("Int64")
            data["sar_id"] = data["sar_id"].astype("Int64")
            data["temporal"] = data["temporal"].astype("Int64")
            data["cuenta_como_baja"] = data["cuenta_como_baja"].astype("Int64")
            data["cuenta_como_alta"] = data["cuenta_como_alta"].astype("Int64")
            data["ecu_capacitacion"] = data["ecu_capacitacion"].astype("bool")
            data["fechor"] = pd.to_datetime(data['fechor'], errors='coerce')
            data["emp_id_ec"] = data["emp_id_ec"].astype("Int64")

            return data
        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleado_modalidad_trabajo(self, data: DataFrame) -> DataFrame:
        try:
            data["id_mod_trab"] = data["id_mod_trab"].astype("Int64")
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["id_mod_trab"] = data["id_mod_trab"].astype("Int64")
            data["fechadesde"] = pd.to_datetime(data['fechadesde'], errors='coerce')
            data["fechahasta"] = pd.to_datetime(data['fechahasta'], errors='coerce')
            data["anulado"] = data["anulado"].astype("Int64")
            data["fechor"] = pd.to_datetime(data['fechor'], errors='coerce')
            data["emp_id_gen"] = data["emp_id_gen"].astype("Int64")
            data["emp_id_fin"] = data["emp_id_fin"].astype("Int64")
            data["fechor_fin"] = pd.to_datetime(data['fechor_fin'], errors='coerce')

            return data
        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleado_servicio(self, data: DataFrame) -> DataFrame:
        try:
            data["empsrv_id"] = data["empsrv_id"].astype("Int64")
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["srv_id"] = data["srv_id"].astype("Int64")
            data["empsrv_fechadesde"] = pd.to_datetime(data['empsrv_fechadesde'], errors='coerce')
            data["empsrv_fechahasta"] = pd.to_datetime(data['empsrv_fechahasta'], errors='coerce')
            data["anulado"] = data["anulado"].astype("Int64")
            data["ecu_id"] = data["ecu_id"].astype("Int64")
            data["emp_id_modifico"] = data["emp_id_modifico"].astype("Int64")
            data["fecha_modificacion"] = pd.to_datetime(data['fecha_modificacion'], errors='coerce')

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleado_superiores(self, data: DataFrame) -> DataFrame:
        try:
            data["id_empsup"] = data["id_empsup"].astype("Int64")
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["emp_idsup"] = data["emp_idsup"].astype("Int64")
            data["fechadesde"] = pd.to_datetime(data['fechadesde'], errors='coerce')
            data["fechahasta"] = pd.to_datetime(data['fechahasta'], errors='coerce')
            data["equ_id"] = data["equ_id"].astype("Float64")
            data["anulado"] = data["anulado"].astype("Int64")
            data["emp_id_modifico"] = data["emp_id_modifico"].astype("Int64")
            data["fecha_modifico"] = pd.to_datetime(data['fecha_modifico'], errors='coerce')

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleado_ubicacion(self, data: DataFrame) -> DataFrame:
        try:
            data["eub_id"] = data["eub_id"].astype("Int64")
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["site_id"] = data["site_id"].astype("Int64")
            data["call_id"] = data["call_id"].astype("Int64")
            data["fechadesde"] = pd.to_datetime(data['fechadesde'], errors='coerce')
            data["fechahasta"] = pd.to_datetime(data['fechahasta'], errors='coerce')
            data["anulado"] = data["anulado"].astype("Int64")
            data["observ"] = data["observ"].astype("object")
            data["eub_emp_id"] = data["eub_emp_id"].astype("Int64")
            data["eub_fecha"] = pd.to_datetime(data['eub_fecha'], errors='coerce')
            data["interno"] = data["interno"].astype("object")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_modalidad_trabajo(self, data: DataFrame) -> DataFrame:
        try:
            data["id_mod_trab"] = data["id_mod_trab"].astype("Int64")
            data["modalidad"] = data["modalidad"].astype("object")
            data["anulado"] = data["anulado"].astype("Int64")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_servicios(self, data: DataFrame) -> DataFrame:
        try:
            data["srv_id"] = data["srv_id"].astype("Int64")
            data["srv_nombre"] = data["srv_nombre"].astype("object")
            data["srv_descripcion"] = data["srv_descripcion"].astype("object")
            data["srv_fechadesde"] = pd.to_datetime(data['srv_fechadesde'], errors='coerce')
            data["srv_fechahasta"] = pd.to_datetime(data['srv_fechahasta'], errors='coerce')
            data["anulado"] = data["anulado"].astype("Int64")
            data["suc_id"] = data["suc_id"].astype("Int64")
            data["sar_id"] = data["sar_id"].astype("Int64")
            data["srv_tipo"] = data["srv_tipo"].astype("Int64")
            data["ceco_id"] = data["ceco_id"].astype("object")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_site(self, data: DataFrame) -> DataFrame:
        try:
            data["site_id"] = data["site_id"].astype("Int64")
            data["site_nombre"] = data["site_nombre"].astype("object")
            data["site_descripcion"] = data["site_descripcion"].astype("object")
            data["anulado"] = data["anulado"].astype("Int64")
            data["suc_id"] = data["suc_id"].astype("Int64")
            data["site_emp_id"] = data["site_emp_id"].astype("Int64")
            data["site_fecha"] = pd.to_datetime(data['site_fecha'], errors='coerce')
            data["inactivo"] = data["inactivo"].astype("bool")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_sociedad(self, data: DataFrame) -> DataFrame:
        try:
            data["sociedad_id"] = data["sociedad_id"].astype("Int64")
            data["sociedad_nombre"] = data["sociedad_nombre"].astype("object")
            data["sociedad_ruc"] = data["sociedad_ruc"].astype("object")
            data["sociedad_pred"] = data["sociedad_pred"].astype("bool")
            data["sociedad_anulado"] = data["sociedad_anulado"].astype("bool")
            data["ini_idsap"] = data["ini_idsap"].astype("Int64")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_subarea(self, data: DataFrame) -> DataFrame:
        try:
            data["sar_id"] = data["sar_id"].astype("Int64")
            data["cue_id"] = data["cue_id"].astype("Int64")
            data["sar_nombre"] = data["sar_nombre"].astype("object")
            data["sar_nombrecorto"] = data["sar_nombrecorto"].astype("object")
            data["anulado"] = data["anulado"].astype("Int64")
            data["sar_rotacion"] = data["sar_rotacion"].astype("Int64")
            data["audit_user"] = data["audit_user"].astype("object")
            data["audit_host"] = data["audit_host"].astype("object")
            data["audit_datetime"] = pd.to_datetime(data['audit_datetime'], errors='coerce')
            data["tipogestion"] = data["tipogestion"].astype("object")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleadocuentalucent(self, data: DataFrame) -> DataFrame:
        try:
            data["luc_agente"] = data["luc_agente"].astype("Int64")
            data["luc_fechadesde"] = pd.to_datetime(data['luc_fechadesde'], errors='coerce')
            data["luc_fechahasta"] = pd.to_datetime(data['luc_fechahasta'], errors='coerce')
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["anulado"] = data["anulado"].astype("Int64")
            data["ori_codigo"] = data["ori_codigo"].astype("Int64")
            data["luc_id"] = data["luc_id"].astype("Int64")
            data["luc_clave"] = data["luc_clave"].astype("object")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_empleadosistemaexterno(self, data: DataFrame) -> DataFrame:
        try:
            data["ese_id"] = data["ese_id"].astype("Int64")
            data["se_id"] = data["se_id"].astype("Int64")
            data["emp_id"] = data["emp_id"].astype("Int64")
            data["ese_login"] = data["ese_login"].astype("object")
            data["ese_fechadesde"] = pd.to_datetime(data['ese_fechadesde'], errors='coerce')
            data["ese_fechahasta"] = pd.to_datetime(data['ese_fechahasta'], errors='coerce')
            data["ese_anulado"] = data["ese_anulado"].astype("Int64")
            data["ese_clave"] = data["ese_clave"].astype("object")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()

    def change_type_sistemaexterno(self, data: DataFrame) -> DataFrame:
        try:
            data["se_id"] = data["se_id"].astype("Int64")
            data["se_nombre"] = data["se_nombre"].astype("object")
            data["se_anulado"] = data["se_anulado"].astype("Int64")
            data["se_inactivo"] = data["se_inactivo"].astype("Int64")
            data["se_todaslascuentas"] = data["se_todaslascuentas"].astype("Int64")
            data["se_formatorut"] = data["se_formatorut"].astype("object")
            data["tipo"] = data["tipo"].astype("object")
            data["se_permitir"] = data["se_permitir"].map({True: 1, False: 0})
            data["se_poneclave"] = data["se_poneclave"].map({True: 1, False: 0})
            data["se_adjunto"] = data["se_adjunto"].map({True: 1, False: 0})
            data["se_ruta_archivo"] = data["se_ruta_archivo"].astype("object")

            return data

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return DataFrame()
