"""
Return csv data as a pandas dataframe.

Parameters
    git_path (string): local path on git repo to data you want to load from.
    
Example
    df = load_CSV('data_csv/test_data.csv')

Taken from: https://kiwidamien.github.io/making-a-python-package-vi-including-data-files.html

supplied by Cherryl Chico <cchico.consultant@adb.org>

"""

import pkg_resources
import pandas as pd

def load_CSV(git_path):
    return pd.read_csv(pkg_resources.resource_stream(__name__, git_path))
