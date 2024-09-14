from .etl_database import DataBase
from .etl_mail import Mail
from .function.get_secret import get_secret_management
from json import loads
from os import path


class Secret(DataBase):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def secret_key(self):
        try:
            data_base = DataBase(self.id_table, self.id_query, self.id_date, self.id_file,
                                 self.id_column, self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            db = data_base.file_secret()
            db_secret_name = db[0]
            db_secret_region = db[1]

            secret_dict = loads(get_secret_management(db_secret_name, db_secret_region))

            return secret_dict

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
