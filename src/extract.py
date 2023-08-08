import pickle
from functools import reduce

from gimie.project import Project
from prefect import flow, task
from rdflib import Graph
from retrieve import read_papers

from config import Location

EXTRACT_QUERY = """

PREFIX schema: <http://schema.org/>

SELECT ?url ?name ?forks (COUNT(?contributor) as ?contributors) ?license ?created_at ?updated_at
WHERE {
    ?url a schema:SoftwareSourceCode ;
        schema:name ?name ;
        schema:license ?license ;
        schema:dateCreated ?created_at ;
        schema:dateModified ?updated_at .

    ?contributor a schema:Person ;
        schema:contributor ?url .
}"""


@task
def extract_metadata(paper: dict) -> Graph:
    proj = Project(paper["repo_url"])
    return proj.to_graph()


@task
def combine_graphs(graph1: Graph, graph2: Graph):
    return graph1 | graph2


@task
def graph_to_table(graph: Graph) -> str:
    csv = graph.query(EXTRACT_QUERY).serialize(format="csv").decode()
    return csv


@task
def save_graph(graph: Graph, target_path: str):
    graph.serialize(destination=target_path, format="turtle")


@task
def save_table(csv: str, target_path: str):
    with open(target_path, "w") as f:
        f.write(csv)


@flow
def extract_flow(location: Location = Location()):
    papers = read_papers(location.filtered_papers)
    papers_meta = map(extract_metadata, papers)
    meta_graph = reduce(combine_graphs(papers_meta))
    meta_table = graph_to_table(meta_graph)
    save_graph(meta_graph, location.metadata)
    save_table(meta_table, location.meta_table)


if __name__ == "__main__":
    extract_flow()
