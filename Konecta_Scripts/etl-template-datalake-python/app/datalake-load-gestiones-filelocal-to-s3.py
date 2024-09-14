from package.etl_upload import Put
from package.etl_utility import Utility

# with xlsx
file = Put(id_table=None, id_query=None, id_date=None, id_file=0, id_column=None, id_secret=0,
           id_service=2, id_mail=0, id_sftp=None)
file.upload_file_xlsx()

utility = Utility(id_table=None, id_query=None, id_date=None, id_file=0, id_column=None, id_secret=None,
                  id_service=None, id_mail=0, id_sftp=None)
utility.move_file_xlsx()

print("successful process")

# with csv
file = Put(id_table=None, id_query=None, id_date=None, id_file=1, id_column=None, id_secret=0,
           id_service=2, id_mail=0, id_sftp=None)
file.upload_file_csv()

utility = Utility(id_table=None, id_query=None, id_date=None, id_file=1, id_column=None, id_secret=None,
                  id_service=None, id_mail=0, id_sftp=None)
utility.move_file_csv()

print("successful process")

# with txt
file = Put(id_table=None, id_query=None, id_date=None, id_file=2, id_column=None, id_secret=0,
           id_service=2, id_mail=0, id_sftp=None)
file.upload_file_txt()

utility = Utility(id_table=None, id_query=None, id_date=None, id_file=2, id_column=None, id_secret=None,
                  id_service=None, id_mail=0, id_sftp=None)
utility.move_file_txt()

print("successful process")
