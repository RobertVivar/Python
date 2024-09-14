from .etl_connect import Connect
from .etl_mail import Mail
from .function.get_table import get_table_sql
from .etl_query import Query
from .function.exec_query import execute_query
from sqlalchemy import create_engine
from os import path


class Insert(Connect):

    def __init__(self, id_table, id_query, id_date, id_file, id_column):
        super().__init__(id_table, id_query, id_date, id_file, id_column)
        self.__pool_size = 0
        self.__max_overflow = -1
        self.__if_exists = "append"
        self.__chunks = 50
        self.__index = False

    def insert_sql(self, table_load):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            cn = connection.connection_sqlserver_insert()

            engine = create_engine(cn, pool_size=self.__pool_size, max_overflow=self.__max_overflow)
            tb = get_table_sql(self.id_table)
            table_load.to_sql(tb[1], engine, schema=tb[0], if_exists=self.__if_exists,
                              chunksize=self.__chunks, index=self.__index)

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

    def insert_exec_sp(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            cn = connection.connection_sqlserver_common()

            query = Query(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column)
            query_str = query.query_sentence()

            execute_query(cn, query_str)

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
