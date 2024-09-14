from .etl_secret import Secret
from .etl_database import DataBase
from .function.get_date import day_yesterday
import pyodbc
import mariadb
import mysql.connector as sql
import psycopg2 as pg


class Connect(DataBase):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def connection_sqlserver_insert(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.sqlserver()
        cn = "mssql+pyodbc://%s:%s@%s:1433/%s?driver=%s" % (db[2],
                                                            db[3],
                                                            db[0],
                                                            db[1],
                                                            db[4])
        return cn

    def connection_sqlserver_common(self):
        secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                        self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        secret_dict = secret.secret_key()

        p_username = secret_dict['username']
        p_password = secret_dict['password']
        p_engine = secret_dict['engine']
        p_host = secret_dict['host']
        p_dbname = secret_dict['dbname']

        cn = pyodbc.connect("DRIVER={%s};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (p_engine,
                                                                                 p_host,
                                                                                 p_dbname,
                                                                                 p_username,
                                                                                 p_password))
        return cn

    def connection_mariadb_common(self):
        secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                        self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        secret_dict = secret.secret_key()

        p_username = secret_dict['username']
        p_password = secret_dict['password']
        p_host = secret_dict['host']
        p_dbname = secret_dict['dbname']
        p_port = secret_dict['port']

        cn = mariadb.connect(host=p_host, port=int(p_port), user=p_username, passwd=p_password, db=p_dbname)

        return cn

    def connection_mysql(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.mysql()
        cn = sql.connect(host=db[0], database=db[1], user=db[2], password=db[3])

        return cn

    def connection_postgresql_common(self):
        secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                        self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        secret_dict = secret.secret_key()

        p_username = secret_dict['username']
        p_password = secret_dict['password']
        p_host = secret_dict['host']
        p_dbname = secret_dict['dbname']
        p_port = secret_dict['port']
        p_scheme = secret_dict['scheme']

        cn = pg.connect(host=p_host, database=p_dbname, user=p_username, password=p_password, port=p_port,
                        options=f'-c search_path={p_scheme}', )
        return cn

    def connection_informix_common(self):
        secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                        self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        secret_dict = secret.secret_key()

        p_dsn = secret_dict['dsn']

        cn = "DSN={0}".format(p_dsn)

        return cn

    def connection_xlsx(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.file_xlsx()
        cn_path = db[2]
        cn_like = db[5]

        return cn_path, cn_like

    def connection_csv(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.file_csv()
        cn_path = db[2]
        cn_like = db[4]

        return cn_path, cn_like

    def connection_txt(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.file_txt()
        cn_path = db[2]
        cn_like = db[4]

        return cn_path, cn_like

    def connection_file_s3(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.aws_service_s3()
        cn_service = db[0]
        cn_bucket = db[1]
        cn_prefix = db[2]
        cn_object = db[3]

        return cn_service, cn_bucket, cn_prefix, cn_object

    def connection_db_s3(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.aws_service_s3()
        cn_service = db[0]
        cn_bucket = db[1]
        cn_prefix = db[2]
        cn_object = db[3]

        cn_paths3 = cn_service + "://" + cn_bucket + cn_prefix + cn_object + ".csv"

        return cn_service, cn_bucket, cn_prefix, cn_object, cn_paths3

    def connection_server_sftp(self):
        data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                             self.id_secret, self.id_service, self.id_mail, self.id_sftp)
        db = data_base.server_sftp()
        cn_server = db[0]
        cn_port = db[1]
        cn_user = db[2]
        cn_pass = db[3]
        cn_path = db[4]
        cn_like = db[6]

        date = day_yesterday(self.id_date)
        date_str = date[0].strftime('%Y%m%d')

        cn_like_name = cn_like.replace('?', date_str)
        print(cn_like_name)
        return cn_server, cn_port, cn_user, cn_pass, cn_path, cn_like_name
