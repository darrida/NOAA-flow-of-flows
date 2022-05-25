import glob
from pprint import pprint
from collections import defaultdict
from pathlib import Path
from prefect import task, get_run_logger
from src.support import initialize_s3_client, aws_load_files_year


@task(retries=5, retry_delay_seconds=5)
def load_year_files(data: dict, region_name: str, bucket_name: str):   
    s3_client = initialize_s3_client(region_name)
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket_name, Body="", Key=f"data/")

    success, failed = aws_load_files_year(
        s3_client=s3_client,
        bucket=bucket_name,
        filepaths_l=data,
    )
    year = str(Path(data[0]).name)[:4]
    print(f"{year} | success: {success}, failed: {failed}")


@task()
def flag_updates(bucket: str, local_dir: str, region_name: str, all: bool) -> dict:
    """Takes individual year and finds file difference between AWS and Local

    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders

    Return (set): Diference between AWS and Local
    """  
    logger = get_run_logger()
    
    if not all:
        years.sort()
        years = years[-1]
        print(f"ONLY Check for updates to {years} related data")

    update_l = []

    s3_client = initialize_s3_client(region_name)
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket, Body="", Key=f"data/")

    # File difference between local and aws for indidivual folder/year
    aws_file_set = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix='data')
    for page in pages:
        list_all_keys = page["Contents"]
        # item arrives in format of 'year/filename'; this removes 'year/'
        file_l = [x["Key"].split("/")[1] for x in list_all_keys]
        for f in file_l:
            aws_file_set.add(f)

    # prep AWS "___complete" files for compare
    aws_version_set = set([x for x in aws_file_set if "___complete" in x])

    # find local version file
    local_files = glob.glob(f"{local_dir}/**/*___complete", recursive=True)
    local_files = sorted(local_files)
    local_file_set = set([Path(x).name for x in local_files])

    update_l = local_file_set.difference(aws_version_set)

    logger.info(f"Update/Changes to Upload: {len(update_l)}")
    logger.info(update_l)

    upload_l = []
    parent_dir = Path(local_files[0]).parent.parent
    for u in update_l:
        upload_l.append(Path(parent_dir) / u[:4] / u)

    return upload_l


@task(retries=3, retry_delay_seconds=5)
def cleanup_confirm_files(bucket_name, region_name, local_dir):
    logger = get_run_logger()
    
    s3_client = initialize_s3_client(region_name)
    data_d = defaultdict(list)
    local_files = glob.glob(f"{local_dir}/**/*___complete", recursive=True)

    for file_ in local_files:
        data_d[Path(file_).name[:4]].append(file_)

    count = 0
    for key, files in data_d.items():
        if len(files) > 1:  # if multiple status files
            files = sorted(files)
            files = files[:-1]  # remove newer file from delete list
            for f in files:
                s3_client.delete_object(Bucket=bucket_name, Key=f"data/{Path(f).name}")
                if Path(f).exists():
                    Path(f).unlink()
                count += 1
    logger.info(f"Cleaned up {count} old '___complete' files.")