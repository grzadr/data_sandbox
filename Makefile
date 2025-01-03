RUN=poetry run
SRC=src
TESTS=tests
CODE=$(SRC) $(TESTS)

fmt:
	$(RUN) black $(CODE)
	$(RUN) isort $(CODE)

lint: fmt
	$(RUN) mypy $(CODE)
	$(RUN) pylint $(CODE)

pytest:
	$(RUN) pytest

test: lint pytest

