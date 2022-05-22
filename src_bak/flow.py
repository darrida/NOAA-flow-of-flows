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
# - Uploads local NOAA temperature csv files to AWS S3 storage
# - Includes the following features (to assist with handling the download of 538,000 [34gb] csv files):
#   - Continue Downloading: If the download is interrupted, the script can pick up where it left off
#   - Find Gaps: If an indidivual file is added to the source for any year, or removed from the server
#     for any year, the script can quickly scan all data in both locations, find the differences
#     and download the missing file(s)
# - Map: Uses map over a list of folders to upload files from each folder in a distributed/parallel fashion
##############################################################################
from pathlib import Path
from prefect import Flow, Parameter
from prefect.executors.dask import LocalDaskExecutor
from prefect.run_configs.local import LocalRun
from prefect.utilities.edges import unmapped
from src.tasks import load_year_files, aws_local_folder_difference, s3_list_folders, local_list_folders


executor = LocalDaskExecutor(scheduler="threads", num_workers=4)
with Flow(name="NOAA files: AWS Upload", executor=executor) as flow:
    #    working_dir = Parameter('WORKING_LOCAL_DIR', default=Path('/mnt/c/Users/benha/data_downloads/noaa_global_temps'))
    working_dir = Parameter("WORKING_LOCAL_DIR", default=str(Path("/home/ben/github/NOAA-file-download/local_data/global-summary-of-the-day-archive/")))
    region_name = Parameter("REGION_NAME", default="us-east-1")
    bucket_name = Parameter("BUCKET_NAME", default="noaa-temperature-data")
    all_folders = Parameter("ALL_FOLDERS", default=True)
    t1_local_folders = local_list_folders(working_dir)
    t2_aws_folders = s3_list_folders(region_name, bucket_name)
    t3_years = aws_local_folder_difference(t2_aws_folders, t1_local_folders, all_folders)
    load_year_files.map(t3_years, unmapped(region_name), unmapped(bucket_name), unmapped(working_dir))

flow.run_config = LocalRun(working_dir="/home/share/github/1-NOAA-Data-Download-Cleaning-Verification/")


if __name__ == "__main__":
    state = flow.run()
    assert state.is_successful()
