from package.etl_upload import Put

# for sftp
file = Put(id_table=None, id_query=None, id_date=0, id_file=None, id_column=None, id_secret=0,
           id_service=5, id_mail=0, id_sftp=0)
file.upload_file_sftp_tos3()

print("successful process")