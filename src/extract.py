"""Extract repository metadata from github/gitlab and save to an RDF file.""" ""
import os
from pathlib import Path
import shutil
import tempfile

from dotenv import load_dotenv
from gimie.project import Project
from prefect import flow, task
from rdflib import Graph

from retrieve import read_papers
from config import Location


def extract_metadata(paper: dict) -> Path:
    """Use github/gitlab API to extract metadata about repo to a temporary file."""
    output_path = tempfile.NamedTemporaryFile(delete=False, suffix=".ttl")
    try:
        proj = Project(paper["repo_url"])
        proj.serialize(format="nt", destination=output_path.name)
    except ValueError:
        pass
    return Path(output_path.name)


@task(retries=3, retry_delay_seconds=5)
def batch_extraction(paper_batch: list[dict]) -> list[Path]:
    """Extract metadata from github/gitlab and save to files."""
    return [extract_metadata(paper) for paper in paper_batch]


def concat_and_cleanup(source_path: Path, target_path: Path):
    """Memory efficient concatenation and cleanup of temporary RDF file"""
    with open(target_path, "ab") as wfd:
        with open(source_path, "rb") as fd:
            shutil.copyfileobj(fd, wfd)
        os.remove(source_path)


@task
def batch_papers(
    papers: list[dict], batch_size: int = 100
) -> list[list[dict]]:
    """Split papers into batches of size batch_size."""
    return [
        papers[i : i + batch_size] for i in range(0, len(papers), batch_size)
    ]


@task
def collect_files(meta_nt_batch: list[list[Path]]) -> list[Path]:
    """Collect metadata files from batches."""
    return [file for batch in meta_nt_batch for file in batch]


@flow
def extract_flow(location: Location = Location()):
    """Extract metadata from github/gitlab and save to an RDF file."""

    # Asynchronously fetch repository metadata for each project
    papers = read_papers(location.pwc_filtered_json)
    batches = batch_papers(papers, batch_size=len(papers) // 32)
    meta_nt_batch = batch_extraction.map(batches)

    # Use nt format (line-based) to concatenate graphs as they are generated
    merged_nt_file = location.repo_rdf.with_suffix(".nt")
    merged_nt_file.unlink(missing_ok=True)
    # Files are lazily concatenated and removed when metadata arrives
    for nt in collect_files(meta_nt_batch):
        concat_and_cleanup(nt, merged_nt_file)

    # Save as turtle to drop duplicate triples
    Graph().parse(merged_nt_file, format="nt").serialize(
        destination=location.repo_rdf, format="turtle"
    )
    merged_nt_file.unlink()


if __name__ == "__main__":
    load_dotenv()
    extract_flow()
