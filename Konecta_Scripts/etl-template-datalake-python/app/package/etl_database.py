from .etl import DML
from .function.get_config import ConfigJson


class DataBase(DML):

    def __init__(self, id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                 id_sftp):
        super().__init__(id_table, id_query, id_date, id_file, id_column, id_secret, id_service, id_mail,
                         id_sftp)

    def file_xlsx(self):
        param_xlsx = ConfigJson().get_content_json(file_json="file")["files"][self.id_file]
        param_file_name = param_xlsx["file_name"]
        param_file_type = param_xlsx["file_type"]
        param_file_ori = param_xlsx["file_path_origin"]
        param_file_prc = param_xlsx["file_path_process"]
        param_file_sheet = param_xlsx["file_sheet"]
        param_file_like = param_xlsx["file_like"]

        return param_file_name, param_file_type, param_file_ori, param_file_prc, param_file_sheet, param_file_like

    def file_csv(self):
        param_xlsx = ConfigJson().get_content_json(file_json="file")["files"][self.id_file]
        param_file_name = param_xlsx["file_name"]
        param_file_type = param_xlsx["file_type"]
        param_file_ori = param_xlsx["file_path_origin"]
        param_file_prc = param_xlsx["file_path_process"]
        param_file_like = param_xlsx["file_like"]

        return param_file_name, param_file_type, param_file_ori, param_file_prc, param_file_like

    def file_txt(self):
        param_xlsx = ConfigJson().get_content_json(file_json="file")["files"][self.id_file]
        param_file_name = param_xlsx["file_name"]
        param_file_type = param_xlsx["file_type"]
        param_file_ori = param_xlsx["file_path_origin"]
        param_file_prc = param_xlsx["file_path_process"]
        param_file_like = param_xlsx["file_like"]

        return param_file_name, param_file_type, param_file_ori, param_file_prc, param_file_like

    def file_secret(self):
        param_secret = ConfigJson().get_content_json(file_json="secret")["secrets"][self.id_secret]
        param_secret_name = param_secret["secret_name"]
        param_region_name = param_secret["region_name"]
        return param_secret_name, param_region_name

    def aws_service_s3(self):
        param_s3 = ConfigJson().get_content_json(file_json="service")["services"][self.id_service]
        param_service = param_s3["service"]
        param_bucket = param_s3["name"]
        param_prefix = param_s3["prefix"]
        param_object = param_s3["object"]

        return param_service, param_bucket, param_prefix, param_object

    def server_sftp(self):
        param_sftp = ConfigJson().get_content_json(file_json="sftp")["sftps"][self.id_sftp]
        param_server = param_sftp["server"]
        param_port = param_sftp["port"]
        param_user = param_sftp["user"]
        param_password = param_sftp["password"]
        param_source_path = param_sftp["source_path"]
        param_target_path = param_sftp["target_path"]
        param_filename = param_sftp["filename"]
        param_filetype = param_sftp["filetype"]

        return param_server, param_port, param_user, param_password, param_source_path, param_target_path, param_filename, param_filetype
