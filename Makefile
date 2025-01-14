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

build_goinit:
	make -C goinit build

init_data:
	$(RUN) python \
	-m cProfile \
	-o output_filename.pstats \
	src/data_sandbox/init_data.py \
	data \
	-n 10000000 \
	-b 500000

init_data_go: build_goinit
	goinit/bin/goinit \
	--base-records 1000000 \
	--dir data_go
