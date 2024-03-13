import datetime
import time
import pytest
from src.aggregateLogsToParquet import run_custom
import boto3
import os
from moto import mock_aws
from pathlib import Path


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    moto_credentials_file_path = (
        Path(__file__).parent.parent.absolute() / "dummy_aws_credentials"
    )
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(moto_credentials_file_path)


@pytest.fixture
def boto3_session(aws_credentials):
    with mock_aws():
        session = boto3.Session(profile_name="foo", region_name="us-east-1")
        yield session


@pytest.fixture
def testing_bucket(boto3_session):
    s3_client = boto3_session.client("s3")
    s3_client.create_bucket(Bucket="development-reporting")
    return "development-reporting"


@pytest.fixture
def testing_log_group(boto3_session):
    cloudwatch_client = boto3_session.client("logs")
    cloudwatch_client.create_log_group(logGroupName="testing-log-group")
    return "testing-log-group"


@pytest.fixture
def testing_log_stream(boto3_session, testing_log_group):
    cloudwatch_client = boto3_session.client("logs")
    cloudwatch_client.create_log_stream(
        logGroupName=testing_log_group, logStreamName="test_log_stream"
    )
    return "test_log_stream"


def test_run_custom(
    boto3_session, testing_bucket, testing_log_group, testing_log_stream
):

    cloudwatch_client = boto3_session.client("logs")
    s3_client = boto3_session.client("s3")

    result = cloudwatch_client.put_log_events(
        logGroupName=testing_log_group,
        logStreamName=testing_log_stream,
        logEvents=[
            {
                "timestamp": int(time.time() * 1000),  # 01-01-2022 at midday
                "message": "test log event",  # The raw, UTF-8, form of the log event message.
            },
        ],
    )

    # Get today's date and tomorrow's date
    today = datetime.datetime.now()
    tomorrow = today + datetime.timedelta(days=1)

    # Convert the dates to strings in 'DD-MM-YYYY' format
    today_str = today.strftime("%d-%m-%Y")
    tomorrow_str = tomorrow.strftime("%d-%m-%Y")

    # Call the run_custom function with the date strings
    assert (
        run_custom(
            testing_log_group,
            today_str,
            tomorrow_str,
            {"TestSchema": {}},
            boto3_session,
            testing_bucket,
        )
        == True
    )

    # Get the list of objects in the bucket
    response = s3_client.list_objects(Bucket=testing_bucket)

    # Check if the bucket contains any objects
    if "Contents" in response:
        # Print the names of the objects
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print(f"No objects found in bucket {testing_bucket}")
