from .etl import DML
from .etl_mail import Mail
from .function.get_config import ConfigJson
from .function.get_delete import delete_by_truncate, delete_by_date
from .function.get_table import get_table_sql
from .function.get_date import day_yesterday
from os import path


class Query(DML):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def query_sentence(self):
        try:
            param_query = ConfigJson().get_content_json(file_json="query")["querys"][self.id_query]
            param_query_sql = param_query["query"]

            return param_query_sql

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return False

    def query_delete_truncate(self):
        try:
            tb = get_table_sql(self.id_table)
            param_query_delete = delete_by_truncate(p_schema=tb[0], p_table_name=tb[1])

            return param_query_delete

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return False

    def query_delete_between(self):
        try:
            tb = get_table_sql(self.id_table)
            dt = day_yesterday(self.id_date)
            param_query_delete = delete_by_date(p_schema=tb[0], p_table_name=tb[1], p_delete_by=tb[2],
                                                p_dates=[dt[0], dt[1]])

            return param_query_delete

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return False
