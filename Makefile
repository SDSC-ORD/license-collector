initialize_git:
	@echo "Initializing git..."
	git init 
	
install: 
	@echo "Installing..."
	poetry install
	poetry run pre-commit install

activate:
	@echo "Activating virtual environment"
	poetry shell

setup: initialize_git install

test:
	pytest

data/raw/repos.json: src/00_extract.py
	@echo "Getting project list..."
	python src/retrieve.py

data/processed/filtered.json: data/raw/repos.json src/01_filter.py
	@echo "Filtering projects..."
	python src/01_filter.py

data/processed/metadata.ttl data/processed/metadata.csv: data/processed/filtered.json src/02_extract.py
	@echo "Extracting metadata..."
	python src/02_extract.py

pipeline: data/processed/metadata.ttl

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
