"""
HeadHunter API extractor.
 
Implements BaseExtractor for hh.ru public REST API.
All HTTP details (session, retry, pagination) are encapsulated here so that
the rest of the pipeline sees only List[Dict].
"""
 
import logging
from typing import Any, Dict, List, Optional
 
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
 
from src.extractors.base_extractor import BaseExtractor
from src.utils.config import hh_api as cfg
 
logger = logging.getLogger(__name__)
 
# ---------------------------------------------------------------------------
# Domain-specific exceptions (narrow hierarchy keeps callers simple)
# ---------------------------------------------------------------------------
 
 
class HHExtractorError(Exception):
    """Base for all hh.ru extractor errors."""
 
 
class HHAPIError(HHExtractorError):
    """Raised when the API returns an unexpected HTTP status."""
 
 
class HHRateLimitError(HHExtractorError):
    """Raised on HTTP 429 — caller should back-off and retry."""
 
 
# ---------------------------------------------------------------------------
# HTTP session factory (Single Responsibility: build once, reuse many times)
# ---------------------------------------------------------------------------
 
 
def _build_session(user_agent: str) -> requests.Session:
    """Return a requests.Session with retry logic and default headers."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504], # 429 - too many req, 5** - server error
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
    )
    return session
 
 
# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------
 
 
class HHExtractor(BaseExtractor):
    """
    Fetches vacancies from api.hh.ru.
 
    Pagination is handled internally; callers only specify ``search_query``
    and ``limit``.  Rate-limiting between pages is inherited from
    ``BaseExtractor._apply_rate_limit``.
 
    Usage::
 
        extractor = HHExtractor()
        vacancies = extractor.run(search_query="Data Engineer", limit=500)
    """
 
    # HH API hard-caps per_page at 100
    _MAX_PER_PAGE = 100
 
    def __init__(
        self,
        base_url: str = cfg.base_url,
        user_agent: str = cfg.user_agent,
        rate_limit_delay: float = cfg.rate_limit_delay,
    ) -> None:
        super().__init__(rate_limit_delay=rate_limit_delay)
        self._base_url = base_url.rstrip("/")
        self._session = _build_session(user_agent)
 
    # ------------------------------------------------------------------
    # BaseExtractor contract
    # ------------------------------------------------------------------
 
    @property
    def source_name(self) -> str:
        return "hh.ru"
 
    def extract_vacancies(
        self,
        search_query: str,
        limit: int = 100,
        area: int = 1,          # 1 = Moscow, 2 = Saint-Petersburg
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        Collect up to *limit* vacancies matching *search_query*.
 
        Args:
            search_query: Text forwarded to the ``text`` API parameter.
            limit: Maximum number of vacancies to return.
            area: hh.ru region ID.
            **kwargs: Any extra query parameters passed verbatim to the API.
 
        Returns:
            List of raw vacancy dicts from the ``items`` array.
        """
        results: List[Dict[str, Any]] = []
        max_pages = (limit + self._MAX_PER_PAGE - 1) // self._MAX_PER_PAGE
 
        for page in range(max_pages):
            remaining = limit - len(results)
            if remaining <= 0:
                break
 
            page_size = min(self._MAX_PER_PAGE, remaining)
            batch = self._fetch_page(
                text=search_query,
                area=area,
                per_page=page_size,
                page=page,
                **kwargs,
            )
 
            if not batch:
                logger.info("No more results at page %d — stopping early", page)
                break
 
            results.extend(batch)
            logger.info(
                "Page %d/%d — fetched %d, total so far %d",
                page + 1,
                max_pages,
                len(batch),
                len(results),
            )
 
            self._apply_rate_limit()
 
        return results[:limit]
 
    # ------------------------------------------------------------------
    # Additional public helpers (useful in ad-hoc scripts / tests)
    # ------------------------------------------------------------------
 
    def fetch_vacancy_detail(self, vacancy_id: str) -> Optional[Dict[str, Any]]:
        """
        Return full detail for a single vacancy or ``None`` on error.
 
        hh.ru detail endpoint has stricter rate limits (≈1 req/s).
        """
        try:
            return self._request(f"/vacancies/{vacancy_id}")
        except HHAPIError as exc:
            logger.warning("Could not fetch detail for %s: %s", vacancy_id, exc)
            return None
 
    # ------------------------------------------------------------------
    # Private HTTP helpers
    # ------------------------------------------------------------------
 
    def _fetch_page(self, **params: Any) -> List[Dict[str, Any]]:
        """Request one page of the /vacancies endpoint."""
        try:
            data = self._request("/vacancies", params=params)
            return data.get("items", [])
        except HHRateLimitError:
            logger.warning("Rate-limit hit — aborting pagination")
            return []
        except HHAPIError as exc:
            logger.error("API error fetching page: %s", exc)
            return []
 
    def _request( 
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 10,
    ) -> Dict[str, Any]:
        """
        Execute a single GET request and return the parsed JSON body.
        Если hh.ru поменяет формат ответа, изменится только этот метод.
        
        Raises:
            HHRateLimitError: on HTTP 429.
            HHAPIError: on any other non-2xx response or network error.
        """
        url = f"{self._base_url}{endpoint}"
        try:
            response = self._session.get(url, params=params, timeout=timeout)
        except requests.exceptions.Timeout as exc:
            raise HHAPIError(f"Request timed out: {url}") from exc
        except requests.exceptions.RequestException as exc:
            raise HHAPIError(f"Network error: {exc}") from exc
 
        if response.status_code == 429:
            raise HHRateLimitError("Rate limit exceeded (HTTP 429)")
 
        if not response.ok:
            raise HHAPIError(
                f"HTTP {response.status_code} from {url}: {response.text[:200]}"
            )
 
        return response.json()  # type: ignore[no-any-return]