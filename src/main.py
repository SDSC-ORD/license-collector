"""
Run all workflows in sequence.
"""
from prefect import flow

from config import Config, Location
from retrieve import retrieve_flow
from extract import extract_flow
from enhance import enhance_flow


@flow
def main_flow(
    config: Config = Config(),
    location: Location = Location(),
):
    retrieve_flow(config, location)
    extract_flow(location)
    enhance_flow(location)


if __name__ == "__main__":
    main_flow()
