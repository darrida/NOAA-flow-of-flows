##############################################################################
# Author: Ben Hammond
# Last Changed: 5/25/2022
#
# REQUIREMENTS
# - Detailed dependencies in pyproject.toml
# - Directly referenced:
#   - prefect, boto3
#
# - Infrastructure:
#   - Prefect: Script is registered as a Prefect flow with api.prefect.io
#     - Source: https://prefect.io
#   - AWS S3: Script retrieves and creates files stored in a S3 bucket
#     - Credentials: Stored localled in default user folder created by AWS CLI
#
# DESCRIPTION
# - compares S3 and local "confirmation" files
# - if any difference is detected, it means that there is a new data file to
#   upload, or an existing data file has a new version
# - the data file associated with the difference is uploaded, and overwrites
#   the existing csv in S3
##############################################################################
from asyncio import wait_for
from pathlib import Path
from prefect import flow
from prefect.task_runners import DaskTaskRunner, SequentialTaskRunner
from src.tasks import cleanup_confirm_files, load_year_files, flag_updates


@flow(name="NOAA files: AWS Upload", task_runner=SequentialTaskRunner()) 
    # task_runner=DaskTaskRunner(cluster_kwargs={'n_workers': 2}))
def main():
    working_dir = str(Path("/home/ben/github/NOAA-file-download/local_data/global-summary-of-the-day-archive/"))
    region_name = "us-east-1"
    bucket_name = "noaa-temperature-data"
    all_folders = True
    
    updates_l = flag_updates(bucket_name, working_dir, region_name, all_folders)
    for data in updates_l.wait().result():
        load_year_files([data], region_name, bucket_name)
    cleanup_confirm_files(bucket_name, region_name, working_dir, wait_for=load_year_files)
    
    if not updates_l:
        return True


if __name__ == "__main__":
    main()
