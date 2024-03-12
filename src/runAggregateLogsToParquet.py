import argparse
import aggregateLogsToParquet
from log_schemas.pageView import pageViewSchema

log_groups = {
    "/application/development-data-val-fe": {"schemas": {"PageView": pageViewSchema}}
}


def main():
    """
    This function parses command line arguments and runs the custom log aggregation process.

    Parameters:
    None

    Returns:
    None

    The function uses argparse to parse command line arguments for the start and end dates.
    These dates are then passed to the run_custom function of the aggregateLogsToParquet module,
    along with the log group name and schemas, for each log group.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    for name, log_group in log_groups.items():
        print(f"Running custom log aggregation for log group {name}")
        aggregateLogsToParquet.run_custom(
            name, args.start, args.end, log_group["schemas"]
        )


if __name__ == "__main__":
    main()
