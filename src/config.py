"""
create Pydantic models
"""
from pathlib import Path
from pydantic import BaseModel


class Location(BaseModel):
    """Specify the locations of inputs and outputs"""

    pwc_url: str = "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz"
    all_papers: str = Path("data/raw/papers_with_code.json.gz")
    filtered_papers: str = Path("data/processed/filtered_papers.json.gz")
    metadata: str = Path("data/processed/projects_meta.ttl")
    meta_table: str = Path("data/processed/projects_meta.csv")


class Config(BaseModel):
    """Specify parameters for the flow"""

    max_papers: int | None = 100
