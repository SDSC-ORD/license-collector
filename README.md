
# License collector

Automatically collect license and other metadata from open-source data science repositories.

## Workflow

The repository contains a Prefect workflow which downloads a list of open source repositories from paperswithcode (see [links between papers and code](https://paperswithcode.com/about)) and runs [gimie](https://github.com/SDSC-ORD/gimie) on each git repository. The extracted metadata from all repositories are then combined into a single RDF graph. A table is also extracted from this graph to provide specific attributes.

## Quick Start
### Set up the environment
1. Install [Poetry](https://python-poetry.org/docs/#installation)
2. Set up the environment:
```bash
make setup
make activate
```
### Install new packages
To install new PyPI packages, run:
```bash
poetry add <package-name>
```

### Run Python scripts
To run the Python scripts to process data, train model, and run a notebook, type the following:
```bash
make pipeline
```

