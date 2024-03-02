# extract data from the instacart dataset which is a bunch of indicidual zips of csv
# files in a zip file and load them into a pandas dataframe. Store this dataframe as
# a parquet file for faster loading in the future.

import pandas as pd
from pathlib import Path
import zipfile

INPUT_FILE = Path("instacart_2017_05_01").resolve()


def read_files(file: Path):
    """Reads a zip file and returns a list of pandas dataframes"""
    with zipfile.ZipFile(file) as z:
        with zipfile.ZipFile(z) as z2:
            return [pd.read_csv(z2.open(f)) for f in z2.namelist()]
