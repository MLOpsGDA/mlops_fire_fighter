import pandas as pd
import requests
import zipfile
import io


def read_data_from_github_zip(url):
    """Reads a CSV file from a zip file stored on GitHub.

    Args:
        url (str): The URL of the zip file on GitHub.

    Returns:
        pandas.DataFrame: The DataFrame containing the CSV data.
    """
    # Send a GET request to download the file
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Read the file content from the response
        content = response.content

        # Create a file-like object from the in-memory content
        file = io.BytesIO(content)

        # Extract the zip file
        with zipfile.ZipFile(file, 'r') as zip_ref:
            csv_filename = zip_ref.namelist()[0]
            with zip_ref.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)
                return df
    else:
        print('Failed to download the file from GitHub.')
        return None


def preprocess_date_columns(df, date_column):
    """Preprocesses a date column in a DataFrame by converting
    it to datetime format and extracting the month.

    Args:
        df (pandas.DataFrame): The DataFrame containing the date column.
        date_column (str): The name of the date column to preprocess.

    Returns:
        pandas.DataFrame: The DataFrame with the preprocessed date column.
    """
    # Convert the date column to datetime format
    df[date_column] = pd.to_datetime(df[date_column], format='%d %b %Y')

    # Extract the month from the date column
    df[date_column + '_Month'] = df[date_column].dt.month

    return df


def preprocess_time_columns(df, time_column):
    """ Preprocesses a time column in a DataFrame by converting it to
    datetime format and assigning labels based on time ranges.

    Args:
        df (pandas.DataFrame): The DataFrame containing the time column.
        time_column (str): The name of the time column to preprocess.

    Returns:
        pandas.DataFrame: The DataFrame with the preprocessed time column.
    """
    # Convert the time column to datetime format
    df[time_column] = pd.to_datetime(df[time_column])

    # Define time ranges and labels
    time_ranges = [
        (pd.to_datetime('00:00:00').time(), pd.to_datetime('04:59:59').time()),
        (pd.to_datetime('05:00:00').time(), pd.to_datetime('11:59:59').time()),
        (pd.to_datetime('12:00:00').time(), pd.to_datetime('16:59:59').time()),
        (pd.to_datetime('17:00:00').time(), pd.to_datetime('20:59:59').time())
    ]
    labels = ['Night', 'Morning', 'Afternoon', 'Evening']

    # Assign labels based on time ranges
    df['PartOfDay'] = 'Night'
    for i in range(1, len(time_ranges)):
        mask = (df[time_column].dt.time >= time_ranges[i][0]) & (
            df[time_column].dt.time <= time_ranges[i][1])
        df.loc[mask, 'PartOfDay'] = labels[i]

    return df


def calculate_attendance_time(df):
    """Calculates the attendance time in minutes and adds a new column
    to the DataFrame.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The modified DataFrame with the new column.
    """
    # Calculate the attendance time in minutes
    df['FirstPumpArriving_AttendanceTime_min'] = (
        df['FirstPumpArriving_AttendanceTime'] / 60
    )

    # Return the modified DataFrame
    return df
