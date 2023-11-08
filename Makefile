bin/python:
	python3 -m venv .
	bin/pip install -e .[test]
	bin/pip install maturin

.PHONY: build
build:
	VIRTUAL_ENV=. bin/maturin develop

test: bin/python
	./tests/run-tikv.sh
	- RUST_BACKTRACE=1 bin/pytest -sv tests/
	pkill -f "tiup"
