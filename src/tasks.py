import os
from datetime import timedelta
from prefect import task
from src.support import initialize_s3_client, aws_load_files_year, aws_local_year_find_difference


####################
# PREFECT WORKFLOW #
####################
@task(log_stdout=True)
def local_list_folders(working_dir: str) -> list:
    return os.listdir(str(working_dir))


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def s3_list_folders(region_name, bucket_name: str) -> list:
    """List folder as root of AWS bucket

    Args:
        s3_client: initated boto3 s3_client object
        bucket_name (str): target AWS bucket

    Return (list): list with AWS bucket root folder names
    """
    s3_client = initialize_s3_client(region_name)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="", Delimiter="/")

    def yield_folders(response):
        for content in response.get("CommonPrefixes", []):
            yield content.get("Prefix")

    folder_list = yield_folders(response)
    # remove '/' from end of each folder name
    return [x.split("/")[0] for x in folder_list]


@task(log_stdout=True, max_retries=5, retry_delay=timedelta(seconds=5))
def aws_local_folder_difference(aws_year_folders: list, local_year_folders: list, all: bool = False) -> set:
    """Finds year folders not yet in AWS

    - Finds folders not yet in AWS
    - Sorts and finds the lowest folder number, then adds a year folder below that number
      - This is to ensure that highest year in AWS is double check if it uploaded all files for it

    Args:
        aws_year_folders (list): year folders present in AWS bucket
        local_year_folders (list): year folders present in local dir

    Return (set): set of folders not in AWS + the year below lowest year
    """
    if all:
        return sorted(set(local_year_folders))
    difference_set = sorted(set(aws_year_folders) - set(local_year_folders))
    difference_set.append(str(int(difference_set[0]) - 1))
    return sorted(difference_set)


@task(log_stdout=True)
def load_year_files(year: str, region_name: str, bucket_name: str, working_dir: str):
    s3_client = initialize_s3_client(region_name)

    # If not exists - creates year folder in aws
    s3_client.put_object(Bucket=bucket_name, Body="", Key=f"{year}/")

    file_diffs = aws_local_year_find_difference(
        s3_client=s3_client, bucket=bucket_name, year=year, local_dir=working_dir
    )
    if file_diffs["file_diff_l"]:
        success, failed = aws_load_files_year(
            s3_client=s3_client,
            bucket=bucket_name,
            year=year,
            local_dir=working_dir,
            files_l=file_diffs["file_diff_l"],
            local_count=file_diffs["local_count"],
            cloud_count=file_diffs["cloud_count"],
        )
        print(f"{year} success: {success}, failed: {failed}")
    return True