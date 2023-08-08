#!/usr/bin/env python
"""Python script to process the data"""

import requests
import gzip
import json
import re

from prefect import flow, task  # type: ignore

from config import Location


@task
def download_paper_list(url: str, target_path: str):
    """Download the list of projects from the PWC website"""
    response = requests.get(url, stream=True)
    with open(target_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)


@task
def read_papers(path: str) -> list[dict]:
    """Read the paperswithcode from a gzipped json file"""
    with gzip.open(path, "r") as f:
        return json.load(f)


@task
def filter_papers(papers: list[dict]) -> list[dict]:
    """Filter papers to only include those with a GitHub or GitLab repo"""
    repo_pattern = re.compile(r"^https?://(www\.)?(github|gitlab)\.com/")
    filtered = filter(lambda x: re.search(repo_pattern, x["repo_url"]), papers)
    return list(filtered)


@task
def save_papers(papers: list[dict], target_path: str):
    """Save the filtered paperswithcode to a gzipped json file"""
    with gzip.open(target_path, "w") as f:
        json.dumps(papers, f)


@flow
def retrieve_flow(location: Location = Location()):
    download_paper_list(location.pwc_url, location.all_papers)
    papers = read_papers(location.all_papers)
    papers = filter_papers(papers)
    save_papers(papers, location.filtered_papers)


if __name__ == "__main__":
    retrieve_flow()
