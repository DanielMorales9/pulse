.PHONY: fmt style verify test

fmt:
	poetry run black .

style:
	poetry run mypy pulse

verify: style test

test:
	pytest tests/