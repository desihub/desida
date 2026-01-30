"""
desida.scripts.github_tags
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

# Optional import - only needed for Markdown tables
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

from desida.github import (
    parse_repo_url,
    get_tags_with_dates,
    count_merged_prs_since,
)

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


def read_repo_urls(filename):
    """Return list of urls loaded from filename.

    Args:
        filename (str): Full path to the input file.

    Returns:
        list[str]: Repository URLs, one per non-empty, non-comment line.

    Raises:
        OSError: If the file cannot be opened or read.

    Empty lines and lines starting with '#' are ignored.
    """
    urls = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


def get_latest_tag_and_date(owner, repo, token=None):
    """Return (tag, date) for the latest tag or (None, None).

    Args:
        owner (str): GitHub org or user name.
        repo (str): GitHub repository name.
        token (str or None): Optional GitHub token.

    Returns:
        tuple[str or None, str or None]: Latest tag name and date (YYYY-MM-DD).
    """
    tags = get_tags_with_dates(owner, repo, token=token)
    if not tags:
        return None, None
    name, dt = tags[-1]
    return name, dt.strftime("%Y-%m-%d")


def process_repo(url, token):
    """Query GitHub for repo URL and return a summary dict.

    Args:
        url (str): GitHub repository URL.
        token (str or None): Optional GitHub token.

    Returns:
        dict: Keys are repo_name, tag, tag_date, merged_prs, error.
    """
    try:
        owner, repo = parse_repo_url(url)
    except ValueError as e:
        print(f"[WARN] Skipping invalid URL: {url} ({e})", file=sys.stderr)
        return {
            "repo_name": None,
            "tag": None,
            "tag_date": None,
            "merged_prs": None,
            "error": str(e),
        }

    tag, tag_date = get_latest_tag_and_date(owner, repo, token=token)

    # If we couldn't find a tag, we still want to count *all* merged PRs
    merged_prs = count_merged_prs_since(owner, repo, tag_date, token=token)

    return {
        "repo_name": repo,
        "tag": tag or "N/A",
        "tag_date": tag_date or "N/A",
        "merged_prs": merged_prs,
        "error": None,
    }


def output_csv(rows, out_fh):
    """Write CSV output for repository rows.

    Args:
        rows (list[dict]): Repository summary rows.
        out_fh (io.TextIOBase): Output file handle.
    """
    writer = csv.writer(out_fh)
    writer.writerow(["Repository", "LatestTag", "TagDate", "PRsSinceTag"])
    for r in rows:
        writer.writerow([r["repo_name"], r["tag"], r["tag_date"], r["merged_prs"]])


def output_markdown(rows, out_fh):
    """Write Markdown table output for repository rows.

    Args:
        rows (list[dict]): Repository summary rows.
        out_fh (io.TextIOBase): Output file handle.

    Raises:
        RuntimeError: If tabulate is not installed.
    """
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


def get_repo_tags(repo_urls, github_token=None):
    """Query GitHub for tag info per repo.

    Args:
        repo_urls (list[str]): GitHub repository URLs.
        github_token (str or None): GitHub access token (minimal scope ok).

    Returns:
        list[dict]: dicts with keys repo_name, tag, tag_date, merged_prs, error.

    Raises:
        SystemExit: If no repository URLs are provided.
    """
    token = github_token or os.getenv("GITHUB_TOKEN")
    if token:
        print("[INFO] Using provided GitHub token for authenticated requests.", file=sys.stderr)
    else:
        print("[INFO] No GitHub token supplied - you are limited to 60 requests/hour.", file=sys.stderr)

    # Use raw token when calling helpers

    if not repo_urls:
        print("[ERROR] Input file contains no repository URLs.", file=sys.stderr)
        sys.exit(1)

    results = []
    for url in repo_urls:
        print(f"[INFO] Processing {url} ...", file=sys.stderr)
        try:
            info = process_repo(url, token)
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

        # Small pause to be nice to the API (especially without a token)
        time.sleep(0.1)

    return results


def parse_args(opts=None):
    """Parse command-line options.

    Args:
        opts (list[str] or None): Optional argv list for testing.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
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
    """Run the GitHub tags summary CLI.

    Args:
        opts (list[str] or None): Optional argv list for testing.
    """
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
                urls.append('https://' + repo)
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
