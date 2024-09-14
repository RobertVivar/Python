from .etl_secret import Secret
from .etl_connect import Connect
from .etl_mail import Mail
from .function.put_object import upload_file, upload_dataframe, upload_file_sftp
from os import path
from pandas import DataFrame


class Put(Connect):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def upload_file_xlsx(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn_source = connection.connection_xlsx()
            cn_path = cn_source[0]
            cn_like = cn_source[1]

            cn_target = connection.connection_file_s3()
            cn_service = cn_target[0]
            cn_bucket = cn_target[1]
            cn_prefix = cn_target[2]

            secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                            self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            secret_dict = secret.secret_key()

            access_key = secret_dict['aws_access_key_id']
            secret_key = secret_dict['aws_secret_access_key']

            upload_file(cn_service, access_key, secret_key, cn_path, cn_like, cn_bucket, cn_prefix[1:])

            return True

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

    def upload_file_csv(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn_source = connection.connection_csv()
            cn_path = cn_source[0]
            cn_like = cn_source[1]

            cn_target = connection.connection_file_s3()
            cn_service = cn_target[0]
            cn_bucket = cn_target[1]
            cn_prefix = cn_target[2]

            secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                            self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            secret_dict = secret.secret_key()

            access_key = secret_dict['aws_access_key_id']
            secret_key = secret_dict['aws_secret_access_key']

            upload_file(cn_service, access_key, secret_key, cn_path, cn_like, cn_bucket, cn_prefix[1:])

            return True

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

    def upload_file_txt(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn_source = connection.connection_txt()
            cn_path = cn_source[0]
            cn_like = cn_source[1]

            cn_target = connection.connection_file_s3()
            cn_service = cn_target[0]
            cn_bucket = cn_target[1]
            cn_prefix = cn_target[2]

            secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                            self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            secret_dict = secret.secret_key()

            access_key = secret_dict['aws_access_key_id']
            secret_key = secret_dict['aws_secret_access_key']

            upload_file(cn_service, access_key, secret_key, cn_path, cn_like, cn_bucket, cn_prefix[1:])

            return True

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

    def upload_dataframe_csv_tos3(self, data: DataFrame):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)

            cn_target = connection.connection_db_s3()
            cn_paths3 = cn_target[4]

            secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                            self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            secret_dict = secret.secret_key()

            access_key = secret_dict['aws_access_key_id']
            secret_key = secret_dict['aws_secret_access_key']

            upload_dataframe(access_key, secret_key, cn_paths3, data)

            return True

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

    def upload_file_sftp_tos3(self):
        try:
            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)

            cn_target = connection.connection_db_s3()
            cn_service = cn_target[0]
            cn_bucket = cn_target[1]
            cn_prefix = cn_target[2]

            connection = Connect(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)

            cn_sftp = connection.connection_server_sftp()
            cn_host = cn_sftp[0]
            cn_port = cn_sftp[1]
            cn_user = cn_sftp[2]
            cn_pass = cn_sftp[3]
            cn_path = cn_sftp[4]
            cn_like_name = cn_sftp[5]

            secret = Secret(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                            self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            secret_dict = secret.secret_key()

            access_key = secret_dict['aws_access_key_id']
            secret_key = secret_dict['aws_secret_access_key']

            upload_file_sftp(cn_host, cn_port, cn_user, cn_pass, cn_service, access_key, secret_key, cn_path,
                             cn_like_name, cn_bucket, cn_prefix[1:])

            return True

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
