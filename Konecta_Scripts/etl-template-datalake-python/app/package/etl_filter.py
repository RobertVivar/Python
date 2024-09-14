from .etl_query import Query
from .etl_mail import Mail
from .function.get_date import day_yesterday, day_yesterday_mdy
from os import path


class Filter(Query):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def between_date(self):
        try:
            param_date = day_yesterday(self.id_date)
            query = Query(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                          self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            return query_str.format(param_date[0], param_date[1])

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

    def between_date_dmy(self):
        try:
            param_date = day_yesterday_mdy(self.id_date)
            query = Query(self.id_table, self.id_query, self.id_date, self.id_file, self.id_column,
                          self.id_secret, self.id_service, self.id_mail, self.id_sftp)
            query_str = query.query_sentence()

            return query_str.format(param_date[0], param_date[1])

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

