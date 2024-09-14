import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import os


class Build:
    """
    Esta clase nos permitira enviar correos a traves de conexiones SMTP SSL.
    """
    __host = ""
    __user = ""
    __password = ""
    __port = 465
    __ssl = True

    def __init__(self, host, user, password, **kwargs):
        """
        Se necesitan parametos para la conexion.
        :param host: ip o nombre del servidor.
        :param user: usuario para la conexion al servidor.
        :param password: contrasela para la conexión al servidor.
        :param kwargs: "port" para indicar el número de puerto por defecto es 465. "ssl" por defecto este parametro es
        True, yq que las conexiones se han configurado como seguras.
        """
        self.__host = host
        self.__user = user
        self.__password = password
        self.__port = kwargs.get("port", self.__port)
        self.__ssl = kwargs.get("ssl", self.__ssl)

    def connection_mail_ssl(self, msg, from_addr, to_addrs):
        """
        Metodo para abrir la conexión al servidor smtp
        :param msg: Debe ser una cadena
        :param from_addr: Correo del usuario
        :param to_addrs: Correos destinatarios
        :return: True o False
        """
        try:
            with smtplib.SMTP_SSL(self.__host, self.__port) as server_mail:
                server_mail.login(self.__user, self.__password)
                server_mail.sendmail(msg=msg, from_addr=from_addr, to_addrs=to_addrs)

            return True
        except smtplib.SMTPException as error:
            traceback_error = error.__traceback__
            class_error = error.__class__
            line_error = traceback_error.tb_lineno
            file_error = traceback_error.tb_frame
            print(error)
            print(traceback_error)
            print(class_error)
            print(line_error)
            print(file_error)
            return False

    def build_mail(self, from_mail="", subject="", to="", body="", type_body="plain", **kwargs):
        """
        Metodo para el envío del correo
        :param from_mail: Usuario desde donde se envia los correos.
        :param subject: Asunto del correo.
        :param to: Correo enviar.
        :param body: Cuerpo del correo.
        :param type_body: Puede ser "plain" o "html"
        :param kwargs: "reply_to" permite responder con el usuario de correo. "cc" sirve para los correos en copia.
        "bcc" para los correos ocultos. "priority" para la configuracion de la prioridad. "attachment" para enviar
        archivos adjuntos, se tiene que ingresar mediante una lista attachment=['path/directory/file.pdf'].
        :return: True o False
        """
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = from_mail
        message['To'] = to

        __all_dest = to.strip()

        # Additional option
        __reply_to = kwargs.get("reply_to")
        __cc = kwargs.get("cc")
        __bcc = kwargs.get("bcc")
        __priority = kwargs.get("priority")
        __attachment = kwargs.get("attachment")

        if __reply_to:
            message['Reply-To'] = __reply_to
            __all_dest += __reply_to.strip()

        if __cc:
            message['Cc'] = __cc
            __all_dest += __cc.strip()

        if __bcc:
            message['Bcc'] = __bcc
            __all_dest += __bcc.strip()

        if __priority:
            message['X-Priority'] = __priority

        __all_dest = __all_dest.strip().split(";")

        message.attach(MIMEText(body, type_body))

        if __attachment:
            __bol = type(__attachment) is list
            if __bol:
                for path in __attachment:
                    with open(path, "rb") as attachment:
                        part = MIMEBase('application', 'octed-stream')
                        part.set_payload(attachment.read())

                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        "attachment", filename=os.path.basename(path)
                    )
                    message.attach(part)

        __body_text = message.as_string()

        __bol_ssl = self.__ssl
        if __bol_ssl:
            self.connection_mail_ssl(msg=__body_text, from_addr=from_mail, to_addrs=__all_dest)

        return True

    def get_ssl(self):
        return self.__ssl

    def __str__(self):
        return "Mail {User: %s, Password: %s, Port: %s, SSL: %s}" % (
            self.__user, self.__password, self.__port, self.__ssl)