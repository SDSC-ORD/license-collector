"""Enhance the metadata with additional information and extract to CSV for visualization."""

from pathlib import Path
from io import StringIO

import pandas as pd
from prefect import flow, task
from rdflib import Graph

from config import Location
from retrieve import read_papers

EXTRACT_QUERY = """

PREFIX schema: <http://schema.org/>

SELECT ?url ?lang (COUNT(?contrib) as ?contributors) ?license ?created_at ?updated_at
WHERE {
    ?url a schema:SoftwareSourceCode ;
        schema:programmingLanguage ?lang ;
        schema:dateCreated ?created_at ;
        schema:dateModified ?updated_at .
    OPTIONAL {
        ?url schema:license ?license ;
    }
    OPTIONAL {
        ?url schema:contributor ?contrib .
    }
}
GROUP BY ?url"""


@task
def graph_to_table(graph: Graph) -> str:
    csv = graph.query(EXTRACT_QUERY).serialize(format="csv").decode()
    return csv


@task
def save_table(csv: str, target_path: Path):
    with open(target_path, "w") as f:
        f.write(csv)


@flow
def enhance_flow(location: Location = Location()):
    """Extract table from repository metadata
    and merge with additional information"""

    # Load repo metadata (Github)
    meta_graph = Graph().parse(location.repo_rdf, format="turtle")
    meta_csv = graph_to_table(meta_graph)
    meta_df = pd.read_csv(StringIO(meta_csv))

    # Load paperswithcode data
    papers = read_papers(location.pwc_filtered_json)
    papers_df = pd.DataFrame(papers)

    # Load license metadata

    # Link datasets
    enhanced_df = pd.merge(
        meta_df, papers_df, left_on="url", right_on="repo_url"
    )

    # Persist to disk
    enhanced_df.to_csv(location.combined_csv, index=False)


if __name__ == "__main__":
    enhance_flow()
