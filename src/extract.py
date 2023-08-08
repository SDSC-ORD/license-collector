"""Extract repository metadata from github/gitlab and save to an RDF file.""" ""
from functools import reduce
from pathlib import Path

from gimie.project import Project
from dotenv import load_dotenv
from prefect import flow, task
from rdflib import Graph
from retrieve import read_papers

from config import Location


@task(task_run_name="extract-{paper[repo_url]}")
def extract_metadata(paper: dict) -> Graph:
    """Use github/gitlab API to extract metadata about repo"""
    try:
        proj = Project(paper["repo_url"])
        return proj.to_graph()
    except ValueError:
        # If the URL is mis-formatted, we just skip the repo
        return Graph()


@task
def combine_graphs(graph1: Graph, graph2: Graph):
    """Union of 2 RDF graphs"""
    return graph1 | graph2


@task
def save_graph(graph: Graph, target_path: Path):
    """Save RDF graph to file"""
    graph.serialize(destination=target_path, format="turtle")


@flow
def extract_flow(location: Location = Location()):
    """Extract metadata from github/gitlab and save to an RDF file."""
    papers = read_papers(location.pwc_filtered_json)
    papers_meta = map(extract_metadata, papers)
    meta_graph = reduce(combine_graphs, papers_meta)
    save_graph(meta_graph, location.repo_rdf)


if __name__ == "__main__":
    load_dotenv()
    extract_flow()
