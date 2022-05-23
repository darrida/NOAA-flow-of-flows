import os
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from prefect import flow, task
from prefect.task_runners import DaskTaskRunner


@task()
def merge_files(files_l, year, working_dir):
    year_df = pd.read_csv(Path(working_dir) / year / 'data' / files_l[0])
    exclude_filename = f"{year}_ts_"
    for file_ in tqdm(files_l[1:], desc=year):
        if exclude_filename in file_:
            continue
        single_df = pd.read_csv(Path(working_dir) / year / 'data' / file_)
        year_df = pd.concat([year_df, single_df])
    year_df.to_csv(Path(working_dir) / year / 'data' / f"{year}_full.csv", index=False)


@flow(task_runner=DaskTaskRunner())
def main():
    working_dir = str(Path("/home/ben/github/NOAA-file-download/local_data/global-summary-of-the-day-archive/"))
    folder_list = os.listdir(str(working_dir))
    for folder in folder_list:
        file_l = os.listdir(str(Path(working_dir) / folder / 'data'))
        merge_files(file_l, folder, working_dir)


if __name__ == '__main__':
    main()
