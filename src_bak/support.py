from datetime import datetime
import logging
import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm


def aws_local_year_find_difference(s3_client: boto3, bucket: str, year: str, local_dir: str) -> dict:
    """Takes individual year and finds file difference between AWS and Local

    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders

    Return (set): Diference between AWS and Local
    """
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket, Body="", Key=f"{year}/")

    # File difference between local and aws for indidivual folder/year
    aws_file_set = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=year)
    for page in tqdm(pages):
        list_all_keys = page["Contents"]
        # item arrives in format of 'year/filename'; this removes 'year/'
        file_l = [x["Key"].split("/")[1] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)

    # List local files for year
    local_file_set = set(os.listdir(str(Path(local_dir) / year / 'data')))
    # print(local_file_set)

    # if local files exist for year, but no AWS files, simply pass on set of local files to upload
    if len(local_file_set) > 1 and len(aws_file_set) == 0:
        return list(local_file_set)

    # List local files not yet in aws bucket/year
    file_difference_set = local_file_set - aws_file_set

    # Subtrack 1 from aws_file_ser, because the folder results from AWS include an empty string as one of the set items
    # print(f'{year} - DIFFERENCE: {len(file_difference_set)} (LOCAL: {len(local_file_set)} | CLOUD: {len(aws_file_set) - 1})')
    return {
        "file_diff_l": list(file_difference_set),
        "local_count": len(local_file_set),
        "cloud_count": len(aws_file_set) - 1,
    }


def s3_upload_file(s3_client: boto3.client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    Args:
        s3_client: initated boto3 s3_client object
        file_name (str): File to upload
        bucket (str): target AWS bucket
        object_name (str): S3 object name. If not specified then file_name is used [Optional]

    Return (bool): True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def aws_load_files_year(
    s3_client: boto3.client, bucket: str, year: str, local_dir: str, files_l: list, local_count: int, cloud_count: int
) -> int:
    """Loads set of csv files into aws

    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders
        files (list): filenames to upload

    Return (tuple): Number of upload success (index 0) and failures (index 1)
    """
    upload_count, failed_count = 0, 0
    for csv_file in tqdm(files_l, desc=f"{year} | local: {local_count} | cloud: {cloud_count}"):
        result = s3_upload_file(
            s3_client=s3_client,
            file_name=str(Path(local_dir) / year / 'data' / csv_file),
            bucket=bucket,
            object_name=f"{year}/{csv_file}",
        )
        if not result:
            with open("failed.txt", "a") as f:
                f.write(f"{year} | {csv_file} | {datetime.now()}")
                f.write("\n")
            failed_count += 1
            continue
        upload_count += 1
    return upload_count, failed_count


def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client("s3", region_name=region_name)
