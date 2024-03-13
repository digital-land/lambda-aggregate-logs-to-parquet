import pytest
from moto import mock_aws
import boto3
import os


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


@pytest.fixture
def testing_bucket(s3_client):
    s3_client.create_bucket(Bucket="development-reporting")
    return "development-reporting"


@pytest.fixture
def cloudwatch_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("logs", region_name="us-east-1")
        yield conn


@pytest.fixture
def testing_log_group(cloudwatch_client):
    cloudwatch_client.create_log_group(logGroupName="testing_log_group")
    return "testing_log_group"
