"""Extract repository metadata from github/gitlab and save to an RDF file.""" ""
import os
from functools import reduce
from pathlib import Path
import shutil
import tempfile
from typing import Iterator

from gimie.project import Project
from dotenv import load_dotenv
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


@task
def dispatch_extraction(papers: dict) -> Iterator[Path]:
    """Extract metadata from github/gitlab and save to an RDF file."""
    from multiprocessing import Pool

    p = Pool(processes=4)
    paths = p.map_async(extract_metadata, papers)
    for path in paths.get():
        yield path


@task
def concat_and_cleanup(source_path: Path, target_path: Path):
    """Memory efficient concatenation and cleanup of temporary RDF file"""
    with open(target_path, "ab") as wfd:
        with open(source_path, "rb") as fd:
            shutil.copyfileobj(fd, wfd)
        os.remove(source_path)


@task
def save_graph(graph: Graph, target_path: Path):
    """Save RDF graph to file"""
    graph.serialize(destination=target_path, format="turtle")


@flow
def extract_flow(location: Location = Location()):
    """Extract metadata from github/gitlab and save to an RDF file."""

    # Fetch repository metadata for each project
    # TODO: Make extraction asynchronous
    papers = read_papers(location.pwc_filtered_json)
    meta_nt_files = dispatch_extraction(papers)

    # Use nt format (line-based) to concatenate graphs as they are generated
    merged_nt_file = location.repo_rdf.with_suffix(".nt")
    merged_nt_file.unlink(missing_ok=True)
    for nt in meta_nt_files:
        concat_and_cleanup(nt, merged_nt_file)

    # Save as turtle to drop duplicate triples
    Graph().parse(merged_nt_file, format="nt").serialize(
        destination=location.repo_rdf, format="turtle"
    )
    merged_nt_file.unlink()


if __name__ == "__main__":
    load_dotenv()
    extract_flow()
