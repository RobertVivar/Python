from .etl_connect import Connect
from .etl_query import Query
from .etl_mail import Mail
from .etl_filter import Filter
from .function.get_datareader import get_data_reader_informix
from pandas import read_sql
from os import path
import warnings


class Select(Connect):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def select_table_mariadb_all_rows(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_mariadb_common()

            query = Query(self.id_table, self.id_query, self.id_date, self.id_file,
                          self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_mariadb_with_filter(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_mariadb_common()

            query = Filter(self.id_table, self.id_query, self.id_date, self.id_file,
                           self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.between_date()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_mysql_all_rows(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_mysql()

            query = Query(self.id_table, self.id_query, self.id_date, self.id_file,
                          self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_mysql_with_filter(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_mysql()

            query = Filter(self.id_table, self.id_query, self.id_date, self.id_file,
                           self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.between_date()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_postgresql_all_rows(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_postgresql_common()

            query = Query(self.id_table, self.id_query, self.id_date, self.id_file,
                          self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_postgresql_with_filter(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_postgresql_common()

            query = Filter(self.id_table, self.id_query, self.id_date, self.id_file,
                           self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.between_date()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_informix_all_rows(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_informix_common()

            query = Query(self.id_table, self.id_query, self.id_date, self.id_file,
                          self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            warnings.filterwarnings('ignore')
            select_data = get_data_reader_informix(cn, query_str)

            return select_data

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

    def select_table_informix_with_filter(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_informix_common()

            query = Filter(self.id_table, self.id_query, self.id_date, self.id_file,
                           self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.between_date_dmy()

            warnings.filterwarnings('ignore')
            select_data = get_data_reader_informix(cn, query_str)

            return select_data

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

    def select_table_sqlserver_all_rows(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_sqlserver_common()

            query = Query(self.id_table, self.id_query, self.id_date, self.id_file,
                          self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            warnings.filterwarnings('ignore')
            select_data = read_sql(query_str, con=cn)

            return select_data

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

    def select_table_sqlserver_with_filter(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn = connection.connection_sqlserver_common()

            query = Filter(self.id_table, self.id_query, self.id_date, self.id_file,
                           self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.between_date()

            warnings.filterwarnings('ignore')
            print(query_str)
            select_data = read_sql(query_str, con=cn)

            return select_data

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