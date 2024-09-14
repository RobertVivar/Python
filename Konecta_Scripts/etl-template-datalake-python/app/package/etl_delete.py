from .etl_connect import Connect
from .etl_mail import Mail
from .etl_query import Query
from .function.exec_query import execute_query
from os import path


class Delete(Connect, Query):

    def __init__(self, id_table, id_query, id_date, id_file, id_column):
        super().__init__(id_table, id_query, id_date, id_file, id_column)

    def delete_truncate(self, msg_boolean):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            cn = connection.connection_sqlserver_common()
            query = Query(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            query_str = query.query_delete_truncate()

            if msg_boolean:
                execute_query(cn=cn, query=query_str)
            else:
                pass

            return True

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return False

    def delete_between(self, msg_boolean):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            cn = connection.connection_sqlserver_common()
            query = Query(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            query_str = query.query_delete_between()

            if msg_boolean:
                execute_query(cn=cn, query=query_str)
            else:
                pass

            return True

        except Exception as err:
            print("EXCEPTION: This exception cant continue.")
            traceback_err = err.__traceback__
            class_error = err.__class__
            line_error = traceback_err.tb_lineno
            file_error = path.split(traceback_err.tb_frame.f_code.co_filename)[1]

            alert_mail = Mail(self.id_table, self.id_query, self.id_date, self.id_file,
                              self.id_column, self.id_secret, self.id_service, self.id_mail)
            alert_mail.send_mail(class_error, err, file_error, line_error)

            return False
