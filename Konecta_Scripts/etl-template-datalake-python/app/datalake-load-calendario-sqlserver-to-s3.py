from package.etl_select import Select
from package.etl_transform import TransformData
from package.etl_upload import Put

# for sqlserver all rows
data = Select(id_table=None, id_query=0, id_date=None, id_file=None, id_column=None, id_secret=1,
              id_service=None, id_mail=0, id_sftp=None)
df_sqlserver = data.select_table_sqlserver_all_rows()

# for sqlserver with filter (Format: YYYY-MM-DD)
# data = Select(id_table=None, id_query=1, id_date=0, id_file=None, id_column=None, id_secret=1,
#               id_service=None, id_mail=0, id_sftp=None)
# df_sqlserver = data.select_table_sqlserver_with_filter()

transform = TransformData(id_table=None, id_query=None, id_date=None, id_file=None, id_column=0,
                          id_secret=None, id_service=None, id_mail=0, id_sftp=None)
data_transform = transform.rename_column(df_sqlserver)

file = Put(id_table=None, id_query=None, id_date=None, id_file=None, id_column=None, id_secret=0,
           id_service=0, id_mail=0, id_sftp=None)
file.upload_dataframe_csv_tos3(data_transform)

print("successful process")
