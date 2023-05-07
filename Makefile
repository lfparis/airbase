# Colours
NC=\033[0m\n
HIGHLIGHT=\033[91m

## usage: print useful commands
usage:
	@echo "$(HIGHLIGHT)Choose a command: $(PWD) $(NC)"
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
	black --line-length 79 --check .

## clean: delete python artifacts
clean:
	python -c "import pathlib; [p.unlink() for p in pathlib.Path('.').rglob('*.py[co]')]"
	python -c "import pathlib; [p.rmdir() for p in pathlib.Path('.').rglob('pytest_cache')]"
	rm -rdf ./dist
	rm -rdf ./build
	rm -rdf  airbase.egg-info