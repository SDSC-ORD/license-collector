"""
create Pydantic models
"""
from pydantic import BaseModel


class Location(BaseModel):
    """Specify the locations of inputs and outputs"""

    pwc_url: str = "https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz"
    all_papers: str = "data/raw/papers_with_code.json.gz"
    filtered_papers: str = "data/processed/filtered_papers.json.gz"
    metadata: str = "data/processed/projects_meta.ttl"
    meta_table: str = "data/processed/projects_meta.csv"

