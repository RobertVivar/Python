from .etl_database import DataBase
from .etl_template import Html
from .etl_build import Build
from .function.get_config import ConfigJson


class Mail(DataBase):
    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def send_mail(self, class_error, err, file_error, line_error):
        template = Html(name_file='template_email_alert')
        html = template.get_html()
        html_format = html.format(class_error, err, file_error, line_error, 'json', 'url')

        config_email = ConfigJson().get_content_json(file_json='mail')['mails'][self.id_mail]

        tool = Build(host=config_email['host'],
                     user=config_email['from'],
                     password=config_email['password'])

        tool.build_mail(from_mail=config_email['from'],
                        subject=config_email['subject'],
                        to=config_email['to'],
                        cc=config_email['cc'],
                        type_body='html',
                        body=html_format)

        return True
