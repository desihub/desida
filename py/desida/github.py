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


def parse_repo_url(url):
    """Extract (owner, repo) from a GitHub URL or repo name.

    Args:
        url (str): GitHub URL, "owner/repo", or "repo".

    Returns:
        tuple[str, str]: (owner, repo) with any trailing ".git" removed.

    Raises:
        ValueError: If the URL or repo name cannot be parsed.
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


def github_get(url, token=None, params=None, timeout=30):
    """Perform a GET with optional token, retry/backoff, and rate-limit handling.

    Args:
        url (str): Full API URL.
        token (str or None): Optional GitHub token.
        params (dict or None): Query parameters.
        timeout (int): Request timeout in seconds.

    Returns:
        requests.Response: Successful response.

    Raises:
        RuntimeError: If rate limited or retries fail.
        requests.HTTPError: For non-retryable HTTP errors.
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


def _get_tag_commit_date(owner, repo, tag_sha, token=None):
    """Return the datetime of the commit that a tag object or sha points to.

    Args:
        owner (str): GitHub org or user name.
        repo (str): GitHub repository name.
        tag_sha (str): Tag object sha or commit sha.
        token (str or None): Optional GitHub token.

    Returns:
        datetime: Commit datetime in UTC.
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


def get_tags_with_dates(owner, repo, token=None):
    """Return list of tags with dates sorted chronologically.

    Args:
        owner (str): GitHub org or user name.
        repo (str): GitHub repository name.
        token (str or None): Optional GitHub token.

    Returns:
        list[tuple[str, datetime]]: (tag_name, tag_date) sorted oldest to newest.
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


def get_merged_prs(owner, repo, token=None):
    """Return mapping of merged PR number to merge datetime.

    Args:
        owner (str): GitHub org or user name, e.g. "desihub".
        repo (str): GitHub repository name, e.g. "desispec".
        token (str or None): Optional GitHub token.

    Returns:
        Dict[int, datetime]: {pr_number: merged_at_datetime}.
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


def count_merged_prs_since(owner, repo, since_iso, token=None):
    """Count merged PRs after since_iso using the Search API.

    Args:
        owner (str): GitHub org or user name, e.g. "desihub".
        repo (str): GitHub repository name, e.g. "desispec".
        since_iso (str or None): ISO timestamp to filter merges after.
        token (str or None): Optional GitHub token.

    Returns:
        int: Total number of merged PRs.
    """
    q = f"repo:{owner}/{repo} is:pr is:merged"
    if since_iso:
        q += f" merged:>{since_iso}"
    params = {"q": q, "per_page": 1}
    url = f"{GITHUB_API}/search/issues"
    resp = github_get(url, token=token, params=params)
    data = resp.json()
    return int(data.get("total_count", 0))


def get_pr_title(owner, repo, pr_number, token=None):
    """Return the title of a pull request by number.

    Args:
        owner (str): GitHub org or user name, e.g. "desihub".
        repo (str): GitHub repository name, e.g. "desispec".
        pr_number (int): Pull request number.
        token (str or None): Optional GitHub token.

    Returns:
        str: Pull request title.
    """
    url = f"{GITHUB_API}/repos/{owner}/{repo}/pulls/{pr_number}"
    data = github_get(url, token=token).json()
    return data.get("title", f"PR #{pr_number}")
