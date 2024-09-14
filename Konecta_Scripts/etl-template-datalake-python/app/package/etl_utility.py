from .etl_database import DataBase
from .etl_mail import Mail
from .function.move_file import move_process
from os import path


class Utility(DataBase):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail, id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail, id_sftp)

    def move_file_xlsx(self):
        try:
            connection = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                                  self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn_file = connection.file_xlsx()
            cn_origin = cn_file[2]
            cn_target = cn_file[3]
            cn_like = cn_file[5]

            move_process(cn_like, cn_origin, cn_target)

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

    def move_file_csv(self):
        try:
            connection = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                                  self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn_file = connection.file_csv()
            cn_origin = cn_file[2]
            cn_target = cn_file[3]
            cn_like = cn_file[4]

            move_process(cn_like, cn_origin, cn_target)

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

    def move_file_txt(self):
        try:
            connection = DataBase(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                                  self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            cn_file = connection.file_csv()
            cn_origin = cn_file[2]
            cn_target = cn_file[3]
            cn_like = cn_file[4]

            move_process(cn_like, cn_origin, cn_target)

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
