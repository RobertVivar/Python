from .get_config import ConfigJson


def get_table_sql(id_table):
    param_table = ConfigJson().get_content_json(file_json="table")["tables"][id_table]
    param_sch = param_table["table_schema"]
    param_nam = param_table["table_name"]
    param_del = param_table["criterion_delete"]

    return param_sch, param_nam, param_del
