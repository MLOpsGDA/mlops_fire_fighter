from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import os
import sys
import yaml
import pandas as pd

root_path = os.path.abspath(os.path.join(os.getcwd(), '.'))
sys.path.append(root_path)

from utils.helpers import (preprocess_date_columns,
                           preprocess_time_columns,
                           calculate_attendance_time)

# Don't forget to run ``az login`` in the command prompt and authenticate!

with open("./config.yml", "r") as f:
    config = yaml.safe_load(f)

ACCOUNT_URL = config['data']['account_url']
CONTAINER_NAME = config['data']['azure_container_name']
ONLINE_ADDRESS = config['data']['online_address']
DATA_PATH = config['data']['current_table']
OLD_DATA_PATH = config['data']['path_old_data']
default_credential = DefaultAzureCredential()

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(ACCOUNT_URL, credential=default_credential)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


def new_data_check():
    """Checks and if necessary updates the training tables in blob storage
    The old tables will moved to the oldies/ directory.
    The check performed ensures incident records consistency and schema preservation
    """

    online_records = pd.read_csv(ONLINE_ADDRESS)
    currently_used_records = pd.read_parquet(BytesIO(container_client.download_blob(DATA_PATH).readall()))

    assert set(currently_used_records['IncidentNumber']).intersection(set(online_records['IncidentNumber'])) == set(currently_used_records['IncidentNumber'])
    assert set(currently_used_records.columns) == set(online_records.columns)

    if len(currently_used_records['IncidentNumber'].unique()) < len(online_records['IncidentNumber'].unique()):
        old_dates =  pd.to_datetime(currently_used_records['DateOfCall'])
        date_min, date_max = old_dates.min().strftime('%Y-%m-%d'), old_dates.max().strftime('%Y-%m-%d'),
        container_client.upload_blob(f'{OLD_DATA_PATH}table_{date_min}-{date_max}.parquet',
                                    currently_used_records.to_parquet(),
                             overwrite=True)
        container_client.upload_blob(DATA_PATH, online_records.to_parquet(), overwrite=True)
        print('New data uploaded')

def preprocess_df(df: pd.DataFrame) -> pd.DataFrame :
    '''Applies the different preprocessing functions to the 
    pandas DF.

    Args:
        - df: input dataframe
    
    Returns:
        A pd DataFrame
    '''
    df = preprocess_date_columns(df, 'DateOfCall')
    df = preprocess_time_columns(df, 'TimeOfCall')
    df = calculate_attendance_time(df)

    df.dropna(subset=['FirstPumpArriving_AttendanceTime_min'], inplace=True)

    return df


def make_train_test():
    """Checks if new data is available on the webpage, and
    saves (overwrites) it to our blob storage if this is the
    case. Moves the old data to the ``/oldies/`` directory
    """
    df = pd.read_parquet(BytesIO(container_client.download_blob(DATA_PATH).readall()))

    training_table = preprocess_df(df)

    training_table['datediff'] = (training_table['DateOfCall'].max() - training_table['DateOfCall']).dt.days

    test = training_table.loc[training_table['datediff'] <= 365]
    train = training_table.loc[(training_table['datediff'] >= 366) & (training_table['datediff'] <= 1095)]

    test.drop(['DateOfCall', 'datediff'], axis=1).to_pickle('test.pkl')
    train.drop(['DateOfCall', 'datediff'], axis=1).to_pickle('train.pkl')

    return train, test

if __name__ == '__main__':
    new_data_check()
    make_train_test()
    print('Data Ingestion complete')