# lambda-aggregate-logs-to-parquet
Repo to hold code for aws Lambda function to aggregate application logs to parquet format and save them to S3

the entry point of this lambda is function.lambda_handler()

## Load the venv
```
source venv/bin/activate
```

## Run the lambda locally
```
    make run_aggregate_logs start="DD/MM/YYYY" end="DD/MM/YYYY"
```
Where start and end are the upper and lower bounds of the dates for the logs to be aggregated