import datetime
from src.LogCombiner import LogCombiner
from log_schemas.pageView import pageViewSchema

reportBucket = "development-reporting"
parquet_file_path = f"s3://{reportBucket}"

log_groups = {
    "/application/development-data-val-fe": {"schemas": {"PageView": pageViewSchema}}
}


def lambda_handler(event, context):
    """
    AWS Lambda function handler.

    Parameters:
    event (dict): AWS Lambda uses this parameter to pass in event data to the handler.
    context (LambdaContext): AWS Lambda uses this parameter to provide runtime information to your handler.

    Returns:
    None

    This function is the entry point for AWS Lambda and it calls the run function.
    """
    run()


def run():
    """
    This function runs the log aggregation process.

    Parameters:
    None

    Returns:
    None

    The function calculates the start and end times for the logs to be combined, which are one day before the current time.
    Then, for each log group, it creates a LogCombiner object and runs the log combining process.
    """
    last_midnight = datetime.datetime.combine(
        datetime.datetime.today(), datetime.time.min
    )
    one_day = datetime.timedelta(days=1)

    for log_group_name, log_group in log_groups:
        logCombiner = LogCombiner(
            log_group_name, log_group.schemas, last_midnight - one_day, last_midnight
        )
        logCombiner.combineLogs(parquet_file_path)


def run_custom(log_group_name, start, end, schemas):
    """
    This function runs the custom log aggregation process.

    Parameters:
    log_group_name (str): The name of the log group.
    start (str): The start date in 'DD-MM-YYYY' or 'DD/MM/YYYY' format.
    end (str): The end date in 'DD-MM-YYYY' or 'DD/MM/YYYY' format.
    schemas (list): The list of schemas for the logs.

    Returns:
    bool: True if the process completes successfully, False otherwise.

    The function first converts the start and end dates from strings to datetime objects.
    If the dates are not in the correct format, it prints an error message and returns False.
    Then, it creates a LogCombiner object and runs the log combining process.
    If the process completes successfully, it returns True.
    """
    formats = ["%d-%m-%Y", "%d/%m/%Y"]

    for fmt in formats:
        try:
            start = datetime.datetime.strptime(start, fmt)
            break
        except ValueError:
            pass
    else:
        print(
            f"Error: The start date is not in the correct format. Please use 'DD-MM-YYYY' or 'DD/MM/YYYY'. provided: {start}"
        )
        return False

    for fmt in formats:
        try:
            end = datetime.datetime.strptime(end, fmt)
            break
        except ValueError:
            pass
    else:
        print(
            f"Error: The end date is not in the correct format. Please use 'DD-MM-YYYY' or 'DD/MM/YYYY'. provided: {end}"
        )
        return False

    start = datetime.datetime.combine(start, datetime.time.min)
    end = datetime.datetime.combine(end, datetime.time.min)

    logCombiner = LogCombiner(log_group_name, schemas, start, end)
    logCombiner.combineLogs(parquet_file_path)

    return True
