# =============================
# Setup
# =============================

init::
	python -m pip install pip-tools
	make piptool-compile
	make dependencies
	python -m pre_commit install

piptool-compile::
	python -m piptools compile --output-file=requirements/requirements.txt requirements/requirements.in
	python -m piptools compile requirements/dev-requirements.in

dependencies::
	pip-sync requirements/requirements.txt requirements/dev-requirements.txt



# =============================
# Execution
# =============================

run_aggregate_logs:
	python3 runAggregateLogsToParquet.py --start $(start) --end $(end)



# =============================
# Linting
# =============================

lint:
	make black
	make flake8

black:
	python3 -m black .

flake8:
	python3 -m flake8 .

jslint::
	npx eslint --ext .html,.js ./

jslint-fix::
	npx eslint --ext .html,.js ./ --fix



# =============================
# Testing
# =============================

test:: test-integration test-acceptance

test-integration::
	python -m pytest tests/integration

test-acceptance::
	python -m pytest -s tests/acceptance
