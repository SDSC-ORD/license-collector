from functools import reduce
from pathlib import Path

from gimie.project import Project
from dotenv import load_dotenv
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
def graph_to_table(graph: Graph) -> str:
    csv = graph.query(EXTRACT_QUERY).serialize(format="csv")
    return csv


@task
def save_table(csv: str, target_path: Path):
    with open(target_path, "wt") as f:
        f.write(csv)


def enhance_flow(location: Location = Location()):
    meta_graph = Graph().parse(location.metadata, format="turtle")
    meta_table = graph_to_table(meta_graph)
    save_table(meta_table, location.meta_table)


if __name__ == "__main__":
    enhance_flow()
