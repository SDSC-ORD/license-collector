"""
Run all workflows in sequence.
"""
from prefect import flow
from dotenv import load_dotenv

from config import Config, Location
from retrieve import retrieve_flow
from extract import extract_flow
from enhance import enhance_flow


def main_flow():
    load_dotenv()
    retrieve_flow()
    extract_flow()
    enhance_flow()


if __name__ == "__main__":
    main_flow()
