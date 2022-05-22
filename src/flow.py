##############################################################################
# Author: Ben Hammond
# Last Changed: 5/7/21
#
# REQUIREMENTS
# - Detailed dependencies in requirements.txt
# - Directly referenced:
#   - prefect, boto3, tqdm
#
# - Infrastructure:
#   - Prefect: Script is registered as a Prefect flow with api.prefect.io
#     - Source: https://prefect.io
#   - AWS S3: Script retrieves and creates files stored in a S3 bucket
#     - Credentials: Stored localled in default user folder created by AWS CLI
#
# DESCRIPTION
# - compares version files in local year/data folder with verion files in aws S3
# - re-uploads relevant entire year folder when version numbers are different
##############################################################################
import os
from pathlib import Path
from prefect import flow
from prefect.task_runners import DaskTaskRunner
from src.tasks import load_year_files, flag_updates, generate_download_list


@flow(name="NOAA files: AWS Upload", task_runner=DaskTaskRunner())
def main():
    working_dir = str(Path("/home/ben/github/NOAA-file-download/local_data/global-summary-of-the-day-archive/"))
    region_name = "us-east-1"
    bucket_name = "noaa-temperature-data"
    chunks = 200
    all_folders = True
    
    folder_list = os.listdir(str(working_dir))
    updates_l = flag_updates(bucket_name, folder_list, working_dir, region_name, all_folders)
    download_l = generate_download_list(updates_l, chunks)
    for data in download_l.wait().result():
        load_year_files(data, region_name, bucket_name, working_dir)


if __name__ == "__main__":
    main()
