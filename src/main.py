"""
Create an iris flow
"""
from prefect import flow
from prefect_dask.task_runners import DaskTaskRunner  # type: ignore

from config import Location
from run_notebook import run_notebook
from src.retrieve import retrieve_flow


@flow
def main_flow(
    location: Location = Location(),
):
    retrieve_flow(location)


if __name__ == "__main__":
    main_flow()
