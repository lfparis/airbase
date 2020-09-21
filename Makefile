# Colors
NC=\x1b[0m
L_GREEN=\x1b[32;01m

## usage: print useful commands
usage:
	@echo "$(L_GREEN)Choose a command: $(PWD) $(NC)"
	@bash -c "sed -ne 's/^##//p' ./Makefile | column -t -s ':' |  sed -e 's/^/ /'"

## release: Release new version
release:
	python setup.py sdist bdist_wheel --universal
	twine upload ./dist/*
	make clean

## test: Run tests
test:
	tox
	make clean

## lint: Lint and format
lint:
	flake8 .
	black --check .

## clean: delete python artifacts
clean:
	python -c "import pathlib; [p.unlink() for p in pathlib.Path('.').rglob('*.py[co]')]"
	python -c "import pathlib; [p.rmdir() for p in pathlib.Path('.').rglob('pytest_cache')]"
	rm -rdf ./dist
	rm -rdf  airbase.egg-info