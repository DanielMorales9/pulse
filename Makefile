.PHONY: fmt style verify test

fmt:
	poetry run black .

style:
	poetry run mypy scheduler

verify: style test

test:
	pytest tests/