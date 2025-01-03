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

profile_init_data:
	$(RUN) python \
	-m cProfile \
	-o output_filename.pstats \
	src/data_sandbox/init_data.py \
	data \
	-n 10000000 \
	-b 500000

