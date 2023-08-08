"""
Create an iris flow
"""
from prefect import flow
from prefect_dask.task_runners import DaskTaskRunner  # type: ignore

from config import Config, Location
from run_notebook import run_notebook
from src.retrieve import retrieve_flow
from src.extract import extract_flow
from src.enhance import enhance_flow


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
