import boto3
import os
import awswrangler as wr
import paramiko
from io import BytesIO
from stat import S_ISDIR, S_ISREG


def upload_file(cn_service, access_key, secret_key, cn_path, cn_like, cn_bucket, cn_prefix):
    client = boto3.client(cn_service, aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    for file in os.listdir(cn_path):
        if cn_like in file:
            file_str = cn_prefix + file
            client.upload_file(cn_path + file, cn_bucket, file_str)

    return True


def upload_dataframe(access_key, secret_key, cn_paths3, df):
    sess = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    wr.s3.to_csv(df=df, path=cn_paths3, dataset=False, boto3_session=sess, index=False, sep='|')

    return True


def upload_file_sftp(cn_host, cn_port, cn_username, cn_password, cn_service, access_key, secret_key, cn_path, cn_like,
                     cn_bucket, cn_prefix):
    client = boto3.client(cn_service, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    transport = paramiko.Transport((cn_host, cn_port))
    transport.connect(username=cn_username, password=cn_password)

    with paramiko.SFTPClient.from_transport(transport) as sftp:
        # change to a subdirectory if required
        sftp.chdir(cn_path)

        for entry in sftp.listdir_attr(""):
            mode = entry.st_mode

            # we have a regular file, not a folder
            if S_ISREG(mode):
                f = entry.filename
                if cn_like in f:
                    with BytesIO() as data:
                        print('Downloading file {0} from SFTP.. to S3'.format(f))

                        sftp.getfo(f, data)
                        data.seek(0)

                        client.upload_fileobj(
                            data,
                            cn_bucket,
                            (cn_prefix+'{0}').format(f.lower())
                        )
                    break

    return True
