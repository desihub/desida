"""
desida.github
=============

Utilities to read a list of GitHub repository URLs and output a table with:
    - repository name (short name, e.g. "desispec")
    - latest tag
    - date of that tag (YEAR-MM-DD)
    - number of pull requests merged after that tag

Adapted from code originally written by the LBL CBorg AI coder
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime
from urllib.parse import urlparse
import re

import requests

# Optional import - only needed for Markdown tables
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None


GITHUB_API = "https://api.github.com"
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    # Authorization header will be added later if a token is supplied
}

default_repo_urls = [
    'https://github.com/desihub/desiutil',
    'https://github.com/desihub/desispec',
    'https://github.com/desihub/desitarget',
    'https://github.com/desihub/redrock',
    'https://github.com/desihub/redrock-templates',
    'https://github.com/desihub/fiberassign',
    'https://github.com/desihub/desisurvey',
    'https://github.com/desihub/desimodel',
    'https://github.com/desihub/specter',
    'https://github.com/desihub/gpu_specter',
    'https://github.com/desihub/specex',
    'https://github.com/desihub/specsim',
    'https://github.com/desihub/desisim',
    'https://github.com/desihub/surveysim',
    'https://github.com/desihub/prospect',
    'https://github.com/desihub/desimeter',
    'https://github.com/desihub/simqso',
    'https://github.com/desihub/speclite',
    'https://github.com/desihub/QuasarNP',
    'https://github.com/desihub/specprod-db',
    'https://github.com/desihub/fastspecfit',
]


def get_auth_headers(token: str | None) -> dict:
    """Return request headers with optional Authorization.

    Args:
        token (str): GitHub token

    Returns headers dictionary
    """
    hdrs = HEADERS.copy()
    if token:
        hdrs["Authorization"] = f"token {token}"
    return hdrs


def read_repo_urls(filename: str) -> list[str]:
    """Read non-empty, non-comment lines from input filename

    Args:
        filename (str): full path to input file

    Returns list of urls

    """
    urls = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


def extract_owner_repo(url: str) -> tuple[str, str]:
    """
    Given a GitHub URL, return (owner, repo_name).

    Example:
        https://github.com/desihub/desispec  -> ("desihub", "desispec")
    """
    parsed = urlparse(url)
    if parsed.netloc not in ("github.com", "www.github.com"):
        raise ValueError(f"Not a GitHub URL: {url}")
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse owner/repo from URL: {url}")
    owner, repo = parts[0], parts[1]
    return owner, repo


def request_with_retry(url: str, headers: dict, params: dict | None = None) -> requests.Response:
    """
    Perform a GET request, handling rate-limit (403) and transient errors.

    If we hit the secondary rate limit (status 403 with a `Retry-After` header),
    we sleep and retry once.
    """
    for attempt in range(3):
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            return resp
        if resp.status_code == 403:
            # Check for secondary rate limit
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                wait = int(retry_after)
                print(f"Rate limited, sleeping {wait}s...", file=sys.stderr)
                time.sleep(wait)
                continue
            # Primary rate limit - show remaining and abort
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", "0"))
            reset_in = max(reset_ts - int(time.time()), 0)
            raise RuntimeError(
                f"GitHub API rate limit exceeded (remaining={remaining}). "
                f"Reset in {reset_in}s. Provide a token to increase limits."
            )
        if resp.status_code in (502, 503, 504):
            # Transient server error - back off a bit
            time.sleep(2 ** attempt)
            continue
        # For other errors, raise an exception with details
        resp.raise_for_status()
    raise RuntimeError(f"Failed to GET {url} after retries.")


def get_latest_release(owner: str, repo: str, headers: dict) -> tuple[str, str] | None:
    """
    Try to fetch the latest *release* (which includes a tag). Returns (tag, date)
    where date is ISO-8601 string. Returns None if no release exists.
    """
    url = f"{GITHUB_API}/repos/{owner}/{repo}/releases/latest"
    try:
        resp = request_with_retry(url, headers)
        data = resp.json()
        tag = data.get("tag_name")
        date = data.get("published_at") or data.get("created_at")
        if tag and date:
            return tag, date
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        else:
            raise
    return None

def get_most_recent_tag(owner: str, repo: str, headers: dict) -> tuple[str, str] | None:
    """
    Fetch tags and select the most recent [v]X.Y[.Z]
    Then resolve its commit date.

    Returns (tag, date) or None if the repo has no tags.
    """

    # ------------------------------------------------------------------
    # Get list of all tags
    # ------------------------------------------------------------------
    url = f"{GITHUB_API}/repos/{owner}/{repo}/tags"
    resp = request_with_retry(url, headers, params={"per_page": 100})
    tags = resp.json()
    if not tags:
        return None

    # ------------------------------------------------------------------
    # 2. Parse tag names as semantic versions and keep the highest one
    # ------------------------------------------------------------------
    version_pat = re.compile(r"^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?$")
    best_tag = None
    best_version = None   # tuple (major, minor, patch)

    for tag in tags:
        name = tag.get("name")
        if not name:
            continue
        m = version_pat.match(name)
        if not m:
            continue
        major = int(m.group(1))
        minor = int(m.group(2)) if m.group(2) is not None else 0
        patch = int(m.group(3)) if m.group(3) is not None else 0
        ver = (major, minor, patch)

        if best_version is None or ver > best_version:
            best_version = ver
            best_tag = tag

    tag_name = best_tag.get("name")
    commit_sha = best_tag.get("commit", {}).get("sha")
    if not (tag_name and commit_sha):
        return None

    # Get commit details to extract the date
    commit_url = f"{GITHUB_API}/repos/{owner}/{repo}/git/commits/{commit_sha}"
    commit_resp = request_with_retry(commit_url, headers)
    commit_data = commit_resp.json()
    # The date can be under 'committer' or 'author' depending on tag type
    date = (
        commit_data.get("committer", {}).get("date")
        or commit_data.get("author", {}).get("date")
    )
    if date:
        # trim to just YEAR-MM-DD
        date = date[0:10]
    else:
        date = 'Unknown'

    return tag_name, date


def get_latest_tag_and_date(owner: str, repo: str, headers: dict) -> tuple[str | None, str | None]:
    """
    Return (tag, date) for the latest tag (or release). If none found, returns (None, None).
    """
    # If we want to use releases instead of tags
    ### release = get_latest_release(owner, repo, headers)
    ### if release:
    ###     return release

    # 2. Fallback to plain tags
    tag, date = get_most_recent_tag(owner, repo, headers)
    if tag:
        return tag, date

    return None, None


def count_merged_prs_since(owner: str, repo: str, since_iso: str | None, headers: dict) -> int:
    """
    Count merged PRs after `since_iso`. If `since_iso` is None, count *all* merged PRs.

    Uses the Search API (which returns a total_count field). The query is limited
    to 1000 results, but we only need the count, which is accurate up to that limit.
    """
    # Build the query
    q = f"repo:{owner}/{repo} is:pr is:merged"
    if since_iso:
        q += f" merged:>{since_iso}"
    params = {"q": q, "per_page": 1}  # we only need the total count
    url = f"{GITHUB_API}/search/issues"
    resp = request_with_retry(url, headers, params=params)
    data = resp.json()
    total = data.get("total_count", 0)
    return total


def process_repo(url: str, headers: dict) -> dict:
    """
    Query GitHub for repo url and return a dict with keys:
        repo_name, tag, tag_date, merged_prs (since tag)
    """
    try:
        owner, repo = extract_owner_repo(url)
    except ValueError as e:
        print(f"[WARN] Skipping invalid URL: {url} ({e})", file=sys.stderr)
        return {
            "repo_name": None,
            "tag": None,
            "tag_date": None,
            "merged_prs": None,
            "error": str(e),
        }

    tag, tag_date = get_latest_tag_and_date(owner, repo, headers)

    # If we couldn't find a tag, we still want to count *all* merged PRs
    merged_prs = count_merged_prs_since(owner, repo, tag_date, headers)

    return {
        "repo_name": repo,
        "tag": tag or "N/A",
        "tag_date": tag_date or "N/A",
        "merged_prs": merged_prs,
        "error": None,
    }


def output_csv(rows: list[dict], out_fh):
    writer = csv.writer(out_fh)
    writer.writerow(["Repository", "LatestTag", "TagDate", "PRsSinceTag"])
    for r in rows:
        writer.writerow([r["repo_name"], r["tag"], r["tag_date"], r["merged_prs"]])


def output_markdown(rows: list[dict], out_fh):
    if tabulate is None:
        raise RuntimeError("tabulate package not installed - install it to use Markdown output.")
    table = [
        [r["repo_name"], r["tag"], r["tag_date"], r["merged_prs"]]
        for r in rows
    ]
    md = tabulate(
        table,
        headers=["Repository", "Latest Tag", "Tag Date", "PRs Since Tag"],
        tablefmt="github",
    )
    out_fh.write(md + "\n")


def get_repo_tags(repo_urls: list[str], github_token=None):
    """Query GitHub for info about tags per repo

    Args:
        repo_urls (list of str): list of GitHub repository URLs

    Options:
        github_token (str): GitHub access token (minimal scope ok)

    Return list of dict(repo_name, tag, tag_date, merged_prs, error)
    """
    token = github_token or os.getenv("GITHUB_TOKEN")
    if token:
        print("[INFO] Using provided GitHub token for authenticated requests.", file=sys.stderr)
    else:
        print("[INFO] No GitHub token supplied - you are limited to 60 requests/hour.", file=sys.stderr)

    # Default GitHub API query header plus optional token
    headers = get_auth_headers(token)

    if not repo_urls:
        print("[ERROR] Input file contains no repository URLs.", file=sys.stderr)
        sys.exit(1)

    results = []
    for url in repo_urls:
        print(f"[INFO] Processing {url} ...", file=sys.stderr)
        try:
            info = process_repo(url, headers)
            if info["error"]:
                print(f"[WARN] {info['error']}", file=sys.stderr)
            results.append(info)
        except Exception as exc:
            print(f"[ERROR] Failed to process {url}: {exc}", file=sys.stderr)
            results.append(
                {
                    "repo_name": None,
                    "tag": "ERROR",
                    "tag_date": "ERROR",
                    "merged_prs": "ERROR",
                    "error": str(exc),
                }
            )
            raise exc

        # Small pause to be nice to the API (especially without a token)
        time.sleep(0.1)

    return results


def parse_args(opts=None):
    parser = argparse.ArgumentParser(
        description="Summarize GitHub repo tags",
        epilog="""Without a token, GitHub API rate limits mean you can only query a few URLs.
Generate a classic token at https://github.com/settings/tokens with no additional scope options selected.""",
        )
    parser.add_argument(
        "-i", "--input",
        help="Path to file with repo URLs (one per line)",
        default=None,
    )
    parser.add_argument(
        "-r", "--repos",
        help="comma separated list of repo names (under desihub, or full GitHub URLs)",
        default=None,
    )
    parser.add_argument(
        "-o", "--output",
        help="Write output to file (default: stdout)",
        default=None,
    )
    parser.add_argument(
        "-f", "--format",
        choices=["csv", "md"],
        default="csv",
        help="Output format: csv (default) or md (Markdown table)",
    )
    parser.add_argument(
        "-t", "--token",
        help="GitHub personal access token (or set GITHUB_TOKEN env var)",
        default=None,
    )
    return parser.parse_args(opts)

def main(opts=None): 
    # opts allows main to be called with a list of command line options instead
    # of using sys.argv; this is mainly for testing.
    args = parse_args(opts)

    # Read GitHub repo URLs from file, or use default list
    if args.input:
        urls = read_repo_urls(args.input)
    elif args.repos:
        urls = list()
        for repo in args.repos.split(','):
            if repo.startswith('https://github.com'):
                urls.append(repo)
            elif repo.startswith('github.com'):
                urls.append('https://'+args.repo)
            else:
                urls.append('https://github.com/desihub/'+repo)
    else:
        urls = default_repo_urls

    # Get info about latest tags per repo
    results = get_repo_tags(urls, github_token=args.token)

    # Choose output destination
    out_fh = open(args.output, "w", newline="", encoding="utf-8") if args.output else sys.stdout

    # Write outputs
    try:
        if args.format == "csv":
            output_csv(results, out_fh)
        else:
            output_markdown(results, out_fh)
    finally:
        if args.output:
            out_fh.close()

# Enable this file to be run as a standalone script, even if desida
# isn't installed or even in $PYTHONPATH
if __name__ == "__main__":
    main()
