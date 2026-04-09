"""
Job scraper for data analysis and AI related postings.

Uses the public Remotive API (https://remotive.com/api/remote-jobs) which
requires no authentication. Results from multiple keyword searches are
aggregated, deduplicated by job id, sorted by publication date (newest first)
and returned as a ranked list.

The `notify_jobs` helper prints the digest to the console and also writes a
dated Markdown file into the `log/` directory (same convention used by
`utils.log`), so the user can review the postings any time.
"""

from datetime import datetime
from pathlib import Path

import requests

from .log import get_logger

logger = get_logger(__name__)

REMOTIVE_API_URL = "https://remotive.com/api/remote-jobs"

# Keywords used to find data analysis and AI related job postings.
SEARCH_KEYWORDS = [
    "data analyst",
    "data scientist",
    "data engineer",
    "machine learning",
    "artificial intelligence",
    "ai engineer",
    "ml engineer",
]


def fetch_jobs_from_remotive(search_term, limit=20, timeout=10):
    """
    Fetch job postings from Remotive for a single search term.

    Args:
        search_term (str): keyword to search for
        limit (int): max number of jobs to request
        timeout (int): HTTP request timeout in seconds

    Returns:
        list[dict]: raw job postings returned by the API (empty on failure)
    """
    try:
        response = requests.get(
            REMOTIVE_API_URL,
            params={"search": search_term, "limit": limit},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        jobs = payload.get("jobs", [])
        logger.info("Fetched {} jobs for keyword '{}'".format(len(jobs), search_term))
        return jobs
    except Exception as exc:
        logger.error("Failed to fetch jobs for '{}': {}".format(search_term, exc))
        return []


def fetch_ai_data_jobs(top_n=10, keywords=None):
    """
    Fetch jobs for all data/AI keywords, deduplicate and return the newest.

    Args:
        top_n (int): number of postings to return
        keywords (list[str] or None): override the default keyword list

    Returns:
        list[dict]: deduplicated job postings, newest first
    """
    keywords = keywords or SEARCH_KEYWORDS
    deduped = {}

    for keyword in keywords:
        for job in fetch_jobs_from_remotive(keyword):
            job_id = job.get("id")
            if job_id is not None and job_id not in deduped:
                deduped[job_id] = job

    sorted_jobs = sorted(
        deduped.values(),
        key=lambda j: j.get("publication_date", ""),
        reverse=True,
    )
    logger.info(
        "Aggregated {} unique jobs across {} keywords; returning top {}".format(
            len(sorted_jobs), len(keywords), top_n
        )
    )
    return sorted_jobs[:top_n]


def _format_job_text(idx, job):
    """Return a plain-text block for one job posting."""
    title = job.get("title", "N/A")
    company = job.get("company_name", "N/A")
    location = job.get("candidate_required_location") or "N/A"
    salary = job.get("salary") or "Not listed"
    pub_date = (job.get("publication_date") or "")[:10]
    url = job.get("url", "")
    return (
        "[{idx}] {title}\n"
        "    Company : {company}\n"
        "    Location: {location}\n"
        "    Salary  : {salary}\n"
        "    Posted  : {pub_date}\n"
        "    URL     : {url}\n"
    ).format(
        idx=idx,
        title=title,
        company=company,
        location=location,
        salary=salary,
        pub_date=pub_date,
        url=url,
    )


def _format_job_markdown(idx, job):
    """Return a Markdown block for one job posting."""
    title = job.get("title", "N/A")
    company = job.get("company_name", "N/A")
    location = job.get("candidate_required_location") or "N/A"
    salary = job.get("salary") or "Not listed"
    pub_date = (job.get("publication_date") or "")[:10]
    url = job.get("url", "")
    return (
        "## {idx}. {title}\n"
        "- **Company**: {company}\n"
        "- **Location**: {location}\n"
        "- **Salary**: {salary}\n"
        "- **Posted**: {pub_date}\n"
        "- **URL**: {url}\n\n"
    ).format(
        idx=idx,
        title=title,
        company=company,
        location=location,
        salary=salary,
        pub_date=pub_date,
        url=url,
    )


def notify_jobs(jobs, output_dir=None):
    """
    Print the job digest to stdout and save it as a dated Markdown file.

    Args:
        jobs (list[dict]): job postings to report
        output_dir (str or Path or None): directory for the digest file.
            Defaults to ``log/`` relative to the current working directory,
            matching the convention used by ``utils.log``.

    Returns:
        pathlib.Path: path to the written digest file
    """
    today = datetime.now().strftime("%Y-%m-%d")

    header = "=== Data/AI Jobs Daily Digest ({}) ===".format(today)
    if jobs:
        body = "\n".join(_format_job_text(i, job) for i, job in enumerate(jobs, start=1))
    else:
        body = "No jobs found today."
    print(header)
    print(body)

    output_dir = Path(output_dir) if output_dir is not None else Path("log")
    output_dir.mkdir(parents=True, exist_ok=True)
    digest_path = output_dir / "jobs_{}.md".format(today)

    with open(digest_path, "w", encoding="utf-8") as f:
        f.write("# Data/AI Jobs Digest - {}\n\n".format(today))
        if jobs:
            for i, job in enumerate(jobs, start=1):
                f.write(_format_job_markdown(i, job))
        else:
            f.write("_No jobs found today._\n")

    logger.info("Job digest written to {}".format(digest_path))
    return digest_path


def run_daily_job_scrape(top_n=10, output_dir=None):
    """
    End-to-end entry point used by the scheduler: fetch and notify.

    Args:
        top_n (int): number of postings to include
        output_dir (str or Path or None): where to write the digest file
    """
    logger.info("Starting daily job scrape.")
    jobs = fetch_ai_data_jobs(top_n=top_n)
    notify_jobs(jobs, output_dir=output_dir)
    logger.info("Finished daily job scrape ({} jobs).".format(len(jobs)))


if __name__ == "__main__":
    # Allow manual invocation for testing, e.g. `python -m utils.job_scraper`
    run_daily_job_scrape()
