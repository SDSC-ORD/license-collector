import os
from typing import NewType
from urllib.parse import quote, urlparse

import requests

Forks = NewType("Forks", int)
Stars = NewType("Stars", int)


def get_github_popularity(path: str) -> tuple[Stars, Forks]:
    """Get star and fork count for input Github repo."""
    resp = requests.get(
        f"https://api.github.com/repos/{path}",
        headers={"Authorization": f"token {os.getenv('GITHUB_TOKEN')}"},
    ).json()
    stars = int(resp.get("stargazers_count", 0))
    forks = int(resp.get("forks_count", 0))
    return stars, forks


def get_gitlab_popularity(path: str) -> tuple[Stars, Forks]:
    """Get star and fork count for input Gitlab repo."""
    resp = requests.get(
        f"https://gitlab.example.com/api/v4/projects/{quote(path)}",
        headers={"Authorization": f"token {os.getenv('GITLAB_TOKEN')}"},
    ).json()
    stars = int(resp.get("star_count", 0))
    forks = int(resp.get("forks_count", 0))
    return stars, forks


def get_popularity(uri: str) -> tuple[Stars, Forks]:
    """Get star and fork count for input repo."""
    path = urlparse(uri).path.strip("/")
    if "github" in uri:
        return get_github_popularity(path)
    elif "gitlab" in uri:
        return get_gitlab_popularity(path)
    else:
        return 0
