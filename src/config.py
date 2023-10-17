"""
create Pydantic models
"""
from pathlib import Path
from pydantic import BaseModel


class Location(BaseModel):
    """Specify the locations of inputs and outputs"""

    pwc_url: str = "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz"
    pwc_json: str = Path("data/raw/papers_with_code.json.gz")
    pwc_filtered_json: str = Path("data/processed/filtered_papers.json.gz")
    repo_rdf: str = Path("data/processed/repo_meta.ttl")
    combined_csv: str = Path("data/final/combined_metadata.csv")
    exclude_list: Path | None = None


class Config(BaseModel):
    """Specify parameters for the flow"""

    max_papers: int | None = None
