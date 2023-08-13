"""Extract repository metadata from github/gitlab and save to an RDF file.""" ""
import asyncio
import gzip
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import AsyncIterator

from dotenv import load_dotenv
from gimie.project import Project
from more_itertools import batched
from multiprocessing import Pool
from prefect import flow, task
from rdflib import Graph

#from retrieve import read_papers
from config import Location


def extract_batch(batch: list[dict]) -> list[Path]:
    """Use github/gitlab API to extract metadata about repo to a temporary file."""
    output_paths = []
    # Extract all outputs using multiprocessing and return list
    with Pool() as pool:
        for output_path in pool.map(extract_metadata, batch):
            output_paths.append(output_path)
    return output_paths


def extract_metadata(paper: dict) -> Path:
    """Use github/gitlab API to extract metadata about repo to a temporary file."""
    output_path = tempfile.NamedTemporaryFile(delete=False, suffix=".ttl")
    while True:
        try:
            proj = Project(paper["repo_url"])
            proj.serialize(format="nt", destination=output_path.name)
        except ValueError:
            break
        except ConnectionError:
            time.sleep(100)
            continue
        break
    return Path(output_path.name)


def concat_and_cleanup(source_path: Path, target_path: Path):
    """Memory efficient concatenation and cleanup of temporary RDF file"""
    with open(target_path, "ab") as wfd:
        with open(source_path, "rb") as fd:
            shutil.copyfileobj(fd, wfd)
        os.remove(source_path)


def read_papers(path: Path) -> list[dict]:
    """Read the paperswithcode from a gzipped json file"""
    with gzip.open(path, "r") as f:
        return json.load(f)


def extract_flow(location: Location = Location()):
    """Extract metadata from github/gitlab and save to an RDF file."""

    # Asynchronously fetch repository metadata for each project
    papers = read_papers(location.pwc_filtered_json)
    batches = batched(papers, 100)
    meta_batches = map(extract_batch, batches)

    # Use nt format (line-based) to concatenate graphs as they are generated
    merged_nt_file = location.repo_rdf.with_suffix(".nt")
    merged_nt_file.unlink(missing_ok=True)
    # Files are lazily concatenated and removed when metadata arrives
    for batch in meta_batches:
        for nt in batch:
            concat_and_cleanup(nt, merged_nt_file)

    # Save as turtle to drop duplicate triples
    Graph().parse(merged_nt_file, format="nt").serialize(
        destination=location.repo_rdf, format="turtle"
    )
    merged_nt_file.unlink()


if __name__ == "__main__":
    load_dotenv()
    extract_flow()
