#-*- coding: utf-8 -*-
import boto3
from prj_logging import *
import os

# boto3.set_stream_logger(name='botocore')

url = 'https://xxxx'
access_key = "x"
secret_key = "xx"

src_bucket = 'test-task-01'
src_prefix = 'bg/'

dst_bucket = 'test-x-ext'


def s3_connection(url, access_key, secret_key):
    logging.info('starting s3 session')
    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.resource('s3', endpoint_url=url)
    return s3


def s3_delete_files(s3_conn, bucket=dst_bucket):
    logging.info(f'CLEANING s3-path: {bucket}')

    s3_bucket = s3_conn.Bucket(bucket)
    for file in s3_bucket.objects.all():
        logging.info(f'deleting s3 object: {file.key}')
        s3_bucket.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': file.key}]})

    logging.info(f'CLEANING competed')


def s3_get_src_files(s3_conn, bucket=src_bucket, pref=src_prefix):
    src_bag_files = []

    logging.info(f'READING s3 bucket: {bucket}, dir: {pref}')
    my_bucket = s3_conn.Bucket(bucket)

    for my_bucket_object in my_bucket.objects.filter(Prefix=pref):
        # logging.info(f'found s3 object: {my_bucket_object.key}')
        if str(my_bucket_object.key).endswith('.bag'):
            logging.info(f'object {my_bucket_object.key} added to working list')
            src_bag_files.append(str(my_bucket_object.key))
            # src_bag_files.append(str(my_bucket_object.key).lstrip(src_prefix))
        else:
            logging.info(f'skipping object {my_bucket_object.key}')
    return src_bag_files


def s3_download_file(s3_conn, bucket=src_bucket, file=None):
    logging.info(f"DOWNLOADING file: {file}")
    if file is None:
        logging.error("No file specified")
        exit(1)

    out_dir = 'src'
    out_file = str(file).split('/')[-1]
    out = f'{out_dir}/{out_file}'

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    logging.info(f"downloading to: {out}")
    bucket = s3_conn.Bucket(bucket)
    bucket.download_file(file, f'{out}')
    return out


def s3_make_dir(s3_conn, bucket=dst_bucket, dir_name=None):
    if dir_name is None:
        logging.error("path cannot be empty")
        exit(1)
    logging.info(f"making s3-path: {bucket}/{dir_name}")
    bucket = s3_conn.Bucket(bucket)
    bucket.put_object(Key=dir_name)


def s3_upload_file(s3_conn, file_name, bucket=dst_bucket, dir_name=None):
    if dir_name is None:
        dir_name = ''
    f = file_name.split('/')[-1]
    dest = f'{dir_name}{f}'
    logging.info(f"uploading file: {file_name} to {bucket}/{dest}")
    s3_conn.Object(bucket, dest).put(Body=open(file_name, 'rb'))


# s3_client = s3_connection(url, access_key=access_key, secret_key=secret_key)
# s3_upload_file(s3_client, 'job.log', dst_bucket, 'm1/m2/m3/')
# print(s3_get_src_files(s3_client, src_bucket, src_prefix))
# s3_client.Bucket(dst_bucket).put_object(Key="m1/m2/m3")
# s3_make_dir(s3_client, dst_bucket, 'm1/m2/m3/')