from os import path
from datetime import datetime, timedelta
import json
import pytz


class ConfigJson(object):
    __root_dir = path.dirname(path.dirname(path.abspath(__name__)))

    def get_content_json(self, file_json: str = 'date') -> dict:
        dir_json = self.__root_dir + f'\\config\\{file_json}.json'
        #dir_json = f'C:\\Users\\amonzon\\Documents\\Tickets\\REPE-17389\\bat-entelperu2-python\\config\\{file_json}.json'
        with open(dir_json, 'r', encoding='utf-8') as json_file:
            j = json_file.read()
        str_to_dict = json.loads(j)

        return str_to_dict

    def get_database_credential(self, id_database: int) -> dict:
        credential: dict = self.get_content_json(file_json="db")["database"][id_database]
        return credential

    def get_table_info(self, id_table: int) -> dict:
        tables: dict = self.get_content_json(file_json="table")["tables"][id_table]
        return tables

    def get_sp_info(self, id_sp: int) -> dict:
        sp: dict = self.get_content_json(file_json="store_procedure")["procedures"][id_sp]
        return sp
