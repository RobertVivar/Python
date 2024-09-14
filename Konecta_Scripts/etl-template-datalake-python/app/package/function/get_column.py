from .get_config import ConfigJson

def get_column_map(id_columns) -> dict:
    columns = ConfigJson().get_content_json(file_json="column")["columns"][id_columns]
    param_column_map = columns["column"]

    return param_column_map
