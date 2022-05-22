import os
from pathlib import Path
from prefect import task, get_run_logger
from tqdm import tqdm
from src.support import initialize_s3_client, aws_load_files_year


####################
# PREFECT WORKFLOW #
####################
@task()
def generate_download_list(data: list, size: int) -> list:
    full_l = []
    for d in data:
        year_chunks = [d['file_list'][i:i + size] for i in range(0, len(d['file_list']), size)]
        for chunk in year_chunks:
            chunk = {'year': d['year'], 'file_list': chunk}
            full_l.append(chunk)
    
    return full_l


@task(retries=5, retry_delay_seconds=5)
def load_year_files(data: dict, region_name: str, bucket_name: str, working_dir: str):
    year, files_l = data['year'], data['file_list']
    
    s3_client = initialize_s3_client(region_name)
    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket_name, Body="", Key=f"{year}/")

    if year:
        success, failed = aws_load_files_year(
            s3_client=s3_client,
            bucket=bucket_name,
            year=year,
            local_dir=working_dir,
            files_l=files_l,
        )
        print(f"{year} success: {success}, failed: {failed}")


@task()
def flag_updates(bucket: str, years: list, local_dir: str, region_name: str, all: bool) -> dict:
    """Takes individual year and finds file difference between AWS and Local

    Args:
        s3_client: initated boto3 s3_client object
        bucket (str): target AWS bucket
        year (str): year to check difference for
        local_dir (str): local directory with year folders

    Return (set): Diference between AWS and Local
    """  
    if not all:
        years.sort()
        years = years[-1]
        print(f"ONLY Check for updates to {year} data")

    update_l = []
    for year in tqdm(years):
        s3_client = initialize_s3_client(region_name)
        # If not exists - creates year folder in aws
        s3_client.put_object(Bucket=bucket, Body="", Key=f"{year}/")

        # File difference between local and aws for indidivual folder/year
        aws_file_set = set()
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=year)
        for page in pages:
            list_all_keys = page["Contents"]
            # item arrives in format of 'year/filename'; this removes 'year/'
            file_l = [x["Key"].split("/")[1] for x in list_all_keys]
            for f in file_l:
                aws_file_set.add(f)

        # find AWS version file
        aws_version = [x for x in aws_file_set if f"{year}_ts_" in x]

        # find local version file
        local_file_set = set(os.listdir(str(Path(local_dir) / year / 'data')))
        local_version = [x for x in local_file_set if f"{year}_ts_" in x]

        if not local_version:
            raise Exception(f"No LOCAL version of {year} found")

        if not local_version == aws_version:
            print('UPDATE', year)
            update_l.append({'year': year, 'file_list': list(local_file_set)})

    return update_l