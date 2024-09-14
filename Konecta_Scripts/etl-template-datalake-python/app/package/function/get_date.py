from datetime import datetime, timedelta
from .get_config import ConfigJson


def day_yesterday(id_date):
    param_date = ConfigJson().get_content_json(file_json="date")["dates"][id_date]
    param_start = param_date["date_start"]
    param_end = param_date["date_end"]

    if param_start and param_end:
        date_time_str_start = param_start + " 00:00:00"
        date_time_str_end = param_end + " 23:59:59"
        date_time_obj_start = datetime.strptime(date_time_str_start, '%Y-%m-%d %H:%M:%S')
        date_time_obj_end = datetime.strptime(date_time_str_end, '%Y-%m-%d %H:%M:%S')
        date_init = (date_time_obj_start - timedelta(days=0)).strftime('%Y-%m-%d 00:00:00')
        date_end = (date_time_obj_end - timedelta(days=0)).strftime('%Y-%m-%d 23:59:59')
        param_start = datetime.strptime(date_init, '%Y-%m-%d %H:%M:%S')
        param_end = datetime.strptime(date_end, '%Y-%m-%d %H:%M:%S')

    else:
        date_init = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        date_end = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d 23:59:59')
        param_start = datetime.strptime(date_init, '%Y-%m-%d %H:%M:%S')
        param_end = datetime.strptime(date_end, '%Y-%m-%d %H:%M:%S')

    return param_start, param_end


def day_yesterday_mdy(id_date):
    param_date = ConfigJson().get_content_json(file_json="date")["dates"][id_date]
    param_start = param_date["date_start"]
    param_end = param_date["date_end"]

    if param_start and param_end:
        date_time_str_start = param_start + " 00:00:00"
        date_time_str_end = param_end + " 23:59:59"
        date_time_obj_start = datetime.strptime(date_time_str_start, '%Y-%m-%d %H:%M:%S')
        date_time_obj_end = datetime.strptime(date_time_str_end, '%Y-%m-%d %H:%M:%S')
        date_init = (date_time_obj_start - timedelta(days=0)).strftime('%m/%d/%Y 00:00:00')
        date_end = (date_time_obj_end - timedelta(days=0)).strftime('%m/%d/%Y 23:59:59')

    else:
        date_init = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y 00:00:00')
        date_end = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y 23:59:59')

    return date_init, date_end

