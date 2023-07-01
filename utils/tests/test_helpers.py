import pytest
import pandas as pd
from pytest import approx
from utils.helpers import (preprocess_date_columns,
                           preprocess_time_columns,
                           calculate_attendance_time)


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame for testing
    data = {
        'Date': ['01 Jan 2022', '15 Feb 2023', '10 Mar 2024'],
        'Value': [10, 20, 30]
    }
    return pd.DataFrame(data)


def test_preprocess_date_columns(sample_dataframe):
    # Define the expected output DataFrame after preprocessing
    expected_output = pd.DataFrame({
        'Date': pd.to_datetime(['01 Jan 2022', '15 Feb 2023', '10 Mar 2024'],
                               format='%d %b %Y'),
        'Value': [10, 20, 30],
        'Date_Month': [1, 2, 3]
    })

    # Call the function to preprocess the date column
    result = preprocess_date_columns(sample_dataframe, 'Date')

    # Check if the result matches the expected output
    assert result.equals(expected_output)


@pytest.fixture
def sample_data():
    # Create a sample DataFrame for testing
    data = {
        'Time': pd.to_datetime(
            ['00:30:00', '08:45:00', '14:20:00', '19:10:00']
        ),
        'Value': [10, 20, 30, 40]
    }
    return pd.DataFrame(data)


def test_preprocess_time_columns(sample_data):
    # Define the expected output DataFrame after preprocessing
    expected_output = pd.DataFrame({
        'Time': pd.to_datetime(
            ['00:30:00', '08:45:00', '14:20:00', '19:10:00']
        ),
        'Value': [10, 20, 30, 40],
        'PartOfDay': ['Night', 'Morning', 'Afternoon', 'Evening']
    })

    # Call the function to preprocess the time column
    result = preprocess_time_columns(sample_data, 'Time')

    # Check if the result matches the expected output
    assert result.equals(expected_output)


def test_preprocess_time_columns_assigns_correct_labels(sample_data):
    # Call the function to preprocess the time column
    result = preprocess_time_columns(sample_data, 'Time')

    # Check if the labels are assigned correctly based on time ranges
    assert result.loc[result['PartOfDay'] == 'Night'].shape[0] == 1
    assert result.loc[result['PartOfDay'] == 'Morning'].shape[0] == 1
    assert result.loc[result['PartOfDay'] == 'Afternoon'].shape[0] == 1
    assert result.loc[result['PartOfDay'] == 'Evening'].shape[0] == 1


@pytest.fixture
def sample_df():
    # Create a sample DataFrame for testing
    data = {
        'FirstPumpArriving_AttendanceTime': [120, 180, 240],
        'Value': [10, 20, 30]
    }
    return pd.DataFrame(data)


def test_calculate_attendance_time(sample_df):
    # Define the expected output DataFrame after calculating attendance time
    expected_output = pd.DataFrame({
        'FirstPumpArriving_AttendanceTime': [120, 180, 240],
        'Value': [10, 20, 30],
        'FirstPumpArriving_AttendanceTime_min': [2, 3, 4]
    })

    # Call the function to calculate attendance time
    result = calculate_attendance_time(sample_df)

    # Check if the result matches the expected output approximately
    assert result['FirstPumpArriving_AttendanceTime_min'].values == approx(
        expected_output['FirstPumpArriving_AttendanceTime_min'].values)


def test_calculate_attendance_time_handles_zero_time(sample_df):
    # Add a zero attendance time value to the sample DataFrame
    sample_df.loc[0, 'FirstPumpArriving_AttendanceTime'] = 0

    # Call the function to calculate attendance time with zero value
    result = calculate_attendance_time(sample_df)

    # Check if the zero attendance time is handled correctly
    assert result.loc[0, 'FirstPumpArriving_AttendanceTime_min'] == 0


def test_calculate_attendance_time_handles_negative_time(sample_df):
    # Add a negative attendance time value to the sample DataFrame
    sample_df.loc[0, 'FirstPumpArriving_AttendanceTime'] = -60

    # Call the function to calculate attendance time with negative value
    result = calculate_attendance_time(sample_df)

    # Check if the negative attendance time is handled correctly
    assert result.loc[0, 'FirstPumpArriving_AttendanceTime_min'] == -1
