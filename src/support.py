from datetime import datetime
from pathlib import Path
import glob
from collections import defaultdict
from prefect import get_run_logger
import boto3
from botocore.exceptions import ClientError


def initialize_s3_client(region_name: str) -> boto3.client:
    return boto3.client("s3", region_name=region_name)


def aws_load_files_year(s3_client: boto3.client, bucket: str, filepaths_l: list) -> int:
    """Loads set of csv files into aws

    Args:
        s3_client: initialized boto3 s3_client object
        bucket (str): target AWS bucket
        filepaths_l (List[str]): list of full filepaths of year archives for upload

    Return (tuple): Number of upload success (index 0) and failures (index 1)
    """
    logger = get_run_logger()
    upload_count, failed_count = 0, 0
    for year in filepaths_l:
        csv_dir = Path(year).parent
        csv_d = {'full': [f"{Path(year).name[:4]}_full.csv"]}
        csv_missing_lat_long = f"{Path(year).name[:4]}_missing_lat_long.csv"
        if (csv_dir / csv_missing_lat_long).exists():
            csv_d['miss_lat_long'] = [csv_missing_lat_long]
        csv_missing_elevation = f"{Path(year).name[:4]}_missing_only_elevation.csv"
        if (csv_dir / csv_missing_elevation).exists():
            csv_d['miss_elevation'] = [csv_missing_elevation]

        result_complete = None
        failed = False
        logger.info(f"BEGIN Upload Data Files: {csv_d}")
        for key, value in csv_d.items():
            logger.info(f"UPLOADING {value[0]}")
            result_csv = s3_upload_file(
                s3_client=s3_client,
                file_name=str(csv_dir / value[0]),
                bucket=bucket,
                object_name=f"data/{value[0]}",
            )
            if result_csv:
                value.append(True)
            else:
                value.append(False)
                failed = True

        confirm_filename = Path(year).name
        if not failed:
            logger.info(f"COMPLETE Upload Data Files: {csv_d}")
            logger.info(f"BEGIN Upload Confirmation: {confirm_filename}")
            result_complete = s3_upload_file(
                s3_client=s3_client,
                file_name=str(year),
                bucket=bucket,
                object_name=f"data/{confirm_filename}",
            )
        else:
            raise Exception(f"UPLOAD FAILED for one or more of: {csv_d}")
        if not result_complete:
            with open("failed.txt", "a") as f:
                f.write(f'{csv_d} | {datetime.now()}')
                f.write("\n")
            failed_count += 1
            continue
        logger.info(f"COMPLETE Upload Confirmation: {confirm_filename}")
        upload_count += 1
    return upload_count, failed_count


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
    logger = get_run_logger()
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logger.error(e)
        # logging.error(e)
        return False
    return True


def local_clean_confirm_files(local_dir):
    data_d = defaultdict(list)
    local_files = glob.glob(f"{local_dir}/**/*___complete", recursive=True)

    for file_ in local_files:
        year = Path(file_).name[:4]
        data_d[year].append(file_)

    count = 0
    for key, files in data_d.items():
        if len(files) > 1:  # if multiple status files
            files = sorted(files)
            files = files[:-1]  # remove newer file from delete list
            for f in files:
                if Path(f).exists():
                    Path(f).unlink()
                count += 1
    return count


def s3_clean_confirmation_files(s3_client, bucket_name):
    year_d = defaultdict(list)
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix='data')
    for page in pages:
        list_all_keys = page["Contents"]
        # item arrives in format of 'year/filename'; this removes 'year/'
        page_l = [x["Key"].split("/")[1] for x in list_all_keys]
        page_l = [x for x in page_l if '___complete' in x]
        for file_ in page_l:
            year_d[file_[:4]].append(file_)
    remove_l = []
    for year, files in year_d.items():
        files = sorted(files)[:-1]
        remove_l += files
    for count, file_ in enumerate(remove_l):
        s3_client.delete_object(Bucket=bucket_name, Key=f"data/{Path(file_).name}")
    return count
    