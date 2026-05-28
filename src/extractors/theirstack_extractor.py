"""
TheirStack API extractor.

Endpoint: POST https://api.theirstack.com/v1/jobs/search
Docs:     https://theirstack.com/en/docs/api-reference/jobs/search_jobs_v1

Credit model: 1 credit per returned job. To avoid paying for duplicates
on re-runs, always pass ``posted_at_gte`` — the extractor stores the
timestamp of the last seen job and uses it as a watermark on the next run.

Salary filtering
----------------
``min_salary_usd`` is passed to the API to exclude jobs without salary
on the server side — no credits are wasted on jobs we would discard.

Field filtering
---------------
TheirStack returns ~100 fields per job. We keep only the fields needed
by the transform layer (companies, cities, skills, vacancies, vacancy_skills).
Nested objects (company_object, locations) are also trimmed.
"""

import logging
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.extractors.base_extractor import BaseExtractor
from src.utils.config import theirstack_api as cfg

logger = logging.getLogger(__name__)

_BASE_URL = cfg.base_url.rstrip("/")
_JOBS_ENDPOINT = "/jobs/search"

# ---------------------------------------------------------------------------
# Fields we actually need for the transform layer
# ---------------------------------------------------------------------------
_KEEP_FIELDS = {
    # vacancy core
    "id",
    "job_title",
    "date_posted",
    "seniority",
    "remote",
    "hybrid",
    # salary
    "min_annual_salary",
    "max_annual_salary",
    "min_annual_salary_usd",
    "max_annual_salary_usd",
    "salary_currency",
    # company
    "company",
    "company_object",
    # location
    "locations",
    # skills
    "technology_slugs",
    # employment
    "employment_statuses",
}

# Nested fields to keep inside company_object
_COMPANY_OBJECT_KEEP = {"name", "industry"}

# Nested fields to keep inside each location
_LOCATION_KEEP = {"city", "country_code", "country_name"}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TheirStackExtractorError(Exception):
    """Base for all TheirStack extractor errors."""


class TheirStackAPIError(TheirStackExtractorError):
    """Raised on unexpected HTTP responses."""


class TheirStackAuthError(TheirStackExtractorError):
    """Raised on HTTP 401/403 — bad or missing API key."""


class TheirStackRateLimitError(TheirStackExtractorError):
    """Raised on HTTP 429 — slow down."""


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------


def _build_session(api_key: str) -> requests.Session:
    """
    Create a requests.Session with retry logic and Bearer auth header.

    Raises:
        TheirStackAuthError: When api_key is empty.
    """
    if not api_key:
        raise TheirStackAuthError(
            "THEIRSTACK_API_KEY is not set. Add it to your .env file."
        )

    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return session


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class TheirStackExtractor(BaseExtractor):
    """
    Fetches job postings from TheirStack API.

    TheirStack uses POST + JSON body for all search parameters.
    Pagination is page-based: ``page=0, 1, 2, ...`` with ``limit`` items each.

    Key parameters
    --------------
    job_title_or        : list of job titles (OR logic)
    job_country_code_or : list of ISO 3166-1 alpha-2 country codes
    posted_at_max_age_days : only jobs posted within N days
    posted_at_gte       : ISO 8601 timestamp — fetch only NEW jobs (saves credits)
    min_salary_usd      : only jobs with salary >= this value (1 = any salary)
    technologies_or     : filter by tech stack slugs (e.g. ["python", "spark"])

    Credit warning
    --------------
    1 credit is charged per returned job. Always use ``posted_at_gte``
    for incremental runs and ``min_salary_usd`` to exclude jobs
    without salary on the server side.
    """

    def __init__(
        self,
        api_key: str = cfg.api_key,
        rate_limit_delay: float = cfg.rate_limit_delay,
        max_per_page: int = cfg.max_per_page,
    ) -> None:
        super().__init__(rate_limit_delay=rate_limit_delay)
        self._max_per_page = min(max_per_page, 500)  # API hard cap
        self._session = _build_session(api_key)

    # ------------------------------------------------------------------
    # BaseExtractor contract
    # ------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        return "theirstack"

    def extract_vacancies(
        self,
        search_query: str,
        limit: int = 500,
        *,
        country_codes: Optional[List[str]] = None,
        posted_at_max_age_days: int = 30,
        posted_at_gte: Optional[str] = None,
        technologies: Optional[List[str]] = None,
        min_salary_usd: int = 1,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Fetch job postings from TheirStack.

        Args:
            search_query:           Job title filter (passed as ``job_title_or``).
            limit:                  Max total jobs to return.
            country_codes:          ISO country codes, e.g. ``["RU", "DE"]``.
            posted_at_max_age_days: Only jobs posted within this many days.
            posted_at_gte:          ISO 8601 UTC timestamp for incremental runs.
            technologies:           Tech stack slugs, e.g. ``["python", "spark"]``.
            min_salary_usd:         Minimum salary in USD. Default 1 = any salary,
                                    excluding jobs without salary. Raise to filter
                                    by real threshold (e.g. 30000).
            **kwargs:               Extra TheirStack filter fields merged into body.

        Returns:
            List of filtered job dicts.
        """
        results: List[Dict[str, Any]] = []
        page = 0

        while len(results) < limit:
            page_size = min(self._max_per_page, limit - len(results))
            batch = self._fetch_page(
                search_query=search_query,
                page=page,
                limit=page_size,
                country_codes=country_codes,
                posted_at_max_age_days=posted_at_max_age_days,
                posted_at_gte=posted_at_gte,
                technologies=technologies,
                min_salary_usd=min_salary_usd,
                **kwargs,
            )

            if not batch:
                logger.info("No more results at page %d — stopping", page)
                break

            results.extend(batch)
            logger.info(
                "Page %d — fetched %d jobs, total so far: %d",
                page, len(batch), len(results),
            )
            page += 1
            self._apply_rate_limit()

        return results[:limit]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _filter_fields(job: Dict[str, Any]) -> Dict[str, Any]:
        """Keep only needed top-level fields and trim nested objects."""
        filtered = {k: v for k, v in job.items() if k in _KEEP_FIELDS}

        # Trim company_object
        if "company_object" in filtered and isinstance(filtered["company_object"], dict):
            filtered["company_object"] = {
                k: v
                for k, v in filtered["company_object"].items()
                if k in _COMPANY_OBJECT_KEEP
            }

        # Trim locations
        if "locations" in filtered and isinstance(filtered["locations"], list):
            filtered["locations"] = [
                {k: v for k, v in loc.items() if k in _LOCATION_KEEP}
                for loc in filtered["locations"]
            ]

        return filtered

    @staticmethod
    def _apply_api_filters(
        body: Dict[str, Any],
        country_codes: Optional[List[str]],
        posted_at_max_age_days: int,
        posted_at_gte: Optional[str],
        technologies: Optional[List[str]],
        min_salary_usd: int,
    ) -> None:
        """Add filters to the API request body."""
        if country_codes:
            body["job_country_code_or"] = country_codes

        # At least one date filter must be set per API requirements
        if posted_at_gte:
            body["posted_at_gte"] = posted_at_gte
        else:
            body["posted_at_max_age_days"] = posted_at_max_age_days

        if technologies:
            body["job_technology_slug_or"] = technologies

        # Exclude jobs without salary on the server side
        body["min_salary_usd"] = min_salary_usd

    def _fetch_page(
        self,
        search_query: str,
        page: int,
        limit: int,
        country_codes: Optional[List[str]],
        posted_at_max_age_days: int,
        posted_at_gte: Optional[str],
        technologies: Optional[List[str]],
        min_salary_usd: int = 1,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """Build request body, call the API, return the filtered ``data`` list."""
        body: Dict[str, Any] = {
            "job_title_or": [search_query],
            "page": page,
            "limit": limit,
            "order_by": [
                {"field": "discovered_at", "desc": True},
                {"field": "date_posted", "desc": True},
            ],
            **kwargs,
        }

        self._apply_api_filters(
            body=body,
            country_codes=country_codes,
            posted_at_max_age_days=posted_at_max_age_days,
            posted_at_gte=posted_at_gte,
            technologies=technologies,
            min_salary_usd=min_salary_usd,
        )

        try:
            data = self._request(_JOBS_ENDPOINT, body)
            return [self._filter_fields(job) for job in data.get("data", [])]
        except TheirStackRateLimitError:
            logger.warning("Rate limit hit — stopping pagination")
            return []
        except TheirStackAPIError as exc:
            logger.error("API error on page %d: %s", page, exc)
            return []

    def _request(
        self,
        endpoint: str,
        body: Dict[str, Any],
        timeout: int = 30,
    ) -> Dict[str, Any]:
        """
        Execute a single POST request and return the parsed JSON.

        Raises:
            TheirStackAuthError:      on HTTP 401 / 403.
            TheirStackRateLimitError: on HTTP 429.
            TheirStackAPIError:       on any other non-2xx or network error.
        """
        url = f"{_BASE_URL}{endpoint}"

        try:
            response = self._session.post(url, json=body, timeout=timeout)
        except requests.exceptions.Timeout as exc:
            raise TheirStackAPIError(f"Request timed out: {url}") from exc
        except requests.exceptions.RequestException as exc:
            raise TheirStackAPIError(f"Network error: {exc}") from exc

        if response.status_code in (401, 403):
            raise TheirStackAuthError(
                f"HTTP {response.status_code} — check THEIRSTACK_API_KEY in .env"
            )
        if response.status_code == 429:
            raise TheirStackRateLimitError("Rate limit exceeded (HTTP 429)")
        if not response.ok:
            raise TheirStackAPIError(
                f"HTTP {response.status_code} from {url}: {response.text[:300]}"
            )

        return response.json()  # type: ignore[no-any-return]