from pandas import read_sql, DataFrame
import pyodbc
import warnings


def get_data_reader_sql(cn, p_query) -> DataFrame:
    try:
        cursor = cn.cursor()
        df = read_sql(p_query, cn)
        print("recovered data, {0} records...Ok".format(len(df)))
        cursor.close()
        del cursor
        cn.close()
        return df
    except Exception as error:
        print(error)


def get_data_reader_informix(db_config, query):
    try:
        cn = pyodbc.connect(db_config)
        cursor = cn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            df = read_sql(query, cn)
        #print("Datos recuperados, {0} registros...Ok".format(len(df)))
        cursor.close()
        del cursor
        cn.close()
        return df
    except Exception as error:
        print(error)
