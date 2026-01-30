"""Shared GitHub helper utilities for desida.

This module centralizes small wrappers used by multiple bin/ and library
scripts: URL parsing, authenticated GET with basic retry/rate-limit handling,
tag listing with dates, merged-PR listing, and small helpers.
"""
from __future__ import annotations

import time
import sys
from datetime import datetime, timezone
from typing import Dict
from urllib.parse import urlparse

import requests

GITHUB_API = "https://api.github.com"
PER_PAGE = 100


def parse_repo_url(url: str) -> tuple[str, str]:
    """Extract (owner, repo) from a GitHub URL or repo name.

    Accepts URLs like "https://github.com/owner/repo", "owner/repo",
    or just "repo" (default to owner=desihub) and returns
    (owner, repo) where ``repo`` has any trailing ``.git`` removed.

    """
    if ("/" not in url) and (not url.startswith("http")):
        return "desihub", url.removesuffix(".git")

    p = urlparse(url)
    if p.netloc not in ("github.com", "www.github.com") and p.scheme not in ("http", "https"):
        # Allow passing just owner/repo as a convenience
        parts = url.strip("/").split("/")
        if len(parts) >= 2:
            owner, repo = parts[0], parts[1].removesuffix(".git")
            return owner, repo
        raise ValueError(f"Not a GitHub URL: {url}")
    parts = p.path.strip("/").split("/")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse owner/repo from URL: {url}")
    owner, repo = parts[0], parts[1].removesuffix(".git")
    return owner, repo


def github_get(url: str, token: str | None = None, params: dict | None = None, timeout: int = 30) -> requests.Response:
    """Perform a GET with optional token, retry/backoff, and rate-limit handling.

    Behaviour:
    - Retries up to 3 times for transient 5xx responses and secondary rate-limit replies
      that include a ``Retry-After`` header.
    - For a primary rate-limit (X-RateLimit-Remaining == "0") raises a RuntimeError
      describing the reset time.
    """
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"token {token}"

    for attempt in range(3):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 200:
            return resp

        if resp.status_code == 403:
            # Secondary rate limit may include Retry-After
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                wait = int(retry_after)
                print(f"Rate limited, sleeping {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue
            # Primary rate limit
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
            reset_in = max(reset_ts - int(time.time()), 0)
            raise RuntimeError(
                f"GitHub API rate limit exceeded (remaining={remaining}). Reset in {reset_in}s. Provide a token to increase limits."
            )

        if resp.status_code in (502, 503, 504):
            time.sleep(2 ** attempt)
            continue

        # Other errors: raise with underlying HTTPError
        resp.raise_for_status()

    raise RuntimeError(f"Failed to GET {url} after retries.")


def _get_tag_commit_date(owner: str, repo: str, tag_sha: str, token: str | None = None) -> datetime:
    """Return the datetime of the commit that a tag object/sha points to.

    Works for annotated tags (tag objects) and lightweight tags (commit sha).
    """
    tag_url = f"{GITHUB_API}/repos/{owner}/{repo}/git/tags/{tag_sha}"
    try:
        tag_obj = github_get(tag_url, token=token).json()
        date_str = tag_obj.get("tagger", {}).get("date")
        if date_str:
            return datetime.fromisoformat(date_str.rstrip("Z")).replace(tzinfo=timezone.utc)
    except requests.HTTPError:
        # fall through to treat as commit
        pass

    commit_url = f"{GITHUB_API}/repos/{owner}/{repo}/commits/{tag_sha}"
    commit_obj = github_get(commit_url, token=token).json()
    date_str = commit_obj["commit"]["committer"]["date"]
    return datetime.fromisoformat(date_str.rstrip("Z")).replace(tzinfo=timezone.utc)


def get_tags_with_dates(owner: str, repo: str, token: str | None = None) -> list[tuple[str, datetime]]:
    """Return list of (tag_name, tag_date) sorted chronologically (oldest->newest).

    Pages through the tags API and resolves the commit date for each tag.
    """
    tags: list[tuple[str, datetime]] = []
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{owner}/{repo}/tags"
        params = {"per_page": PER_PAGE, "page": page}
        data = github_get(url, token=token, params=params).json()
        if not data:
            break
        for tag in data:
            name = tag["name"]
            sha = tag["commit"]["sha"]
            tag_date = _get_tag_commit_date(owner, repo, sha, token=token)
            tags.append((name, tag_date))
        page += 1

    tags.sort(key=lambda t: t[1])
    return tags


def get_merged_prs(owner: str, repo: str, token: str | None = None) -> Dict[int, datetime]:
    """Return mapping {pr_number: merged_at_datetime} for merged PRs.

    Pages the pulls API and collects `merged_at` timestamps.
    """
    prs: Dict[int, datetime] = {}
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{owner}/{repo}/pulls"
        params = {"state": "closed", "per_page": PER_PAGE, "page": page}
        data = github_get(url, token=token, params=params).json()
        if not data:
            break
        for pr in data:
            merged_at = pr.get("merged_at")
            if merged_at:
                merged_dt = datetime.fromisoformat(merged_at.rstrip("Z")).replace(tzinfo=timezone.utc)
                prs[pr["number"]] = merged_dt
        page += 1
    return prs


def count_merged_prs_since(owner: str, repo: str, since_iso: str | None, token: str | None = None) -> int:
    """Count merged PRs after `since_iso` using the Search API (returns total_count).

    If `since_iso` is None, counts all merged PRs.
    """
    q = f"repo:{owner}/{repo} is:pr is:merged"
    if since_iso:
        q += f" merged:>{since_iso}"
    params = {"q": q, "per_page": 1}
    url = f"{GITHUB_API}/search/issues"
    resp = github_get(url, token=token, params=params)
    data = resp.json()
    return int(data.get("total_count", 0))


def get_pr_title(owner: str, repo: str, pr_number: int, token: str | None = None) -> str:
    """Return the title of a pull request by number."""
    url = f"{GITHUB_API}/repos/{owner}/{repo}/pulls/{pr_number}"
    data = github_get(url, token=token).json()
    return data.get("title", f"PR #{pr_number}")
