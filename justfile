# Justfile for tolteca_db

# Show available commands
list:
    @just --list

# Run all the formatting, linting, and testing commands
qa:
    uv run --extra test pre-commit run --all-files
    uv run --extra test ty check .
    uv run --extra test pytest .

# Run coverage, and build to HTML
coverage:
    uv run --extra test coverage run -m pytest .
    uv run --extra test coverage report -m
    uv run --extra test coverage html

# Build the project, useful for checking that packaging is correct
build:
    rm -rf build
    rm -rf dist
    uv build

# remove build artifacts
clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -fr {} +

# remove Python file artifacts
clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

# remove test and coverage artifacts
clean-test:
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

# remove all build, test, coverage and Python artifacts
clean: clean-build clean-pyc clean-test
    @echo "Cleaned all build, test, coverage, and Python artifacts."

# Build the docs
doc-build:
    uv run --extra dev sphinx-build -M html docs docs/_build -T

# Serve docs locally
doc: doc-build
    uv run --extra dev mkdocs serve -a localhost:3000

# Deploy docs
doc-deploy: doc-build
    uv run --extra dev mkdocs gh-deploy --force
