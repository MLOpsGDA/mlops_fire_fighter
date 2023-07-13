from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import re
import pytest
import yaml
import pandas as pd
import numpy as np
from pathlib import Path
from io import BytesIO

with open("./config.yml", "r") as f:
    config = yaml.safe_load(f)

ACCOUNT_URL = config['data']['account_url']
CONTAINER_NAME = config['data']['azure_container_name']
ONLINE_ADDRESS = config['data']['online_address']
DATA_PATH = config['data']['current_table']
OLD_DATA_PATH = config['data']['path_old_data']
LOCAL_TRAIN_PATH = config['data']['local_train_data']
LOCAL_TEST_PATH = config['data']['local_test_data']

default_credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(ACCOUNT_URL,
                                        credential=default_credential)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


@pytest.fixture
def current_table():
    """Return table currently used in
    training/testing
    """
    current_table = pd.read_parquet(
        BytesIO(container_client.download_blob(DATA_PATH).readall()))
    return current_table


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame for testing
    data = {
        'DateOfCall': ["01 May 2020", "01 May 2020"],
        'TimeOfCall': ['00:03:18', '00:03:32'],
        'FirstPumpArriving_AttendanceTime': [np.NAN, 293.0],
    }
    return pd.DataFrame(data)


def test_new_data_check(current_table):
    """Test the execution of the new_data_check function"""
    blob_list = container_client.list_blobs()

    files = []
    for blob in blob_list:
        files.append(blob.name)

    assert len(files) > 0

    oldies_paths = re.compile('oldies/')
    data_path = re.compile(f'{DATA_PATH}')
    results_oldies_search, results_data_path_search = [], []
    older_dataframes_paths = []

    for file in files:
        result_oldies = oldies_paths.search(file)
        if result_oldies is not None:
            older_dataframes_paths.append(file)
        result_data_path = data_path.search(file)
        results_oldies_search.append(result_oldies)
        results_data_path_search.append(result_data_path)

    assert len([x for x in results_data_path_search if x is not None]) == 1
    assert len([x for x in results_oldies_search if x is not None]) != 0

    older_dataframes = []
    for old_pd in older_dataframes_paths:
        df = pd.read_parquet(
            BytesIO(container_client.download_blob(old_pd).readall()))
        older_dataframes.append(df)

    for df in older_dataframes:
        assert set(df['IncidentNumber'])\
            .intersection(set(current_table['IncidentNumber'])) == set(
            df['IncidentNumber'])
        assert set(df.columns) == set(current_table.columns)
        assert len(df['IncidentNumber'].unique()) < len(
            current_table['IncidentNumber'].unique())


def test_make_train_test(current_table):
    '''Test the execution of the make_train_test
    function.'''
    train_path = Path(f"./{LOCAL_TRAIN_PATH}")
    test_path = Path(f"./{LOCAL_TEST_PATH}")

    assert train_path.is_file()
    assert test_path.is_file()

    train = pd.read_pickle(train_path)
    test = pd.read_pickle(test_path)
    assert ((pd.to_datetime(current_table['DateOfCall']).max() -
             test['DateOfCall']).dt.days).all() <= 365
    assert ((pd.to_datetime(current_table['DateOfCall']).max() -
             train['DateOfCall']).dt.days).min() >= 366
    assert ((pd.to_datetime(current_table['DateOfCall']).max() -
            train['DateOfCall']).dt.days).max() <= 1095
