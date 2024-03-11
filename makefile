init::
	python -m pip install pip-tools
	make dependencies

dependencies::
	pip-sync requirements/requirements.txt

run_aggregate_logs:
	python3 runAggregateLogsToParquet.py --start $(start) --end $(end)
