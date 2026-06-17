"""Hook API DPE ADEME (connexion Airflow `ademe_api`).

Endpoint : GET https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines
"""
from __future__ import annotations

from airflow.sdk.bases.hook import BaseHook
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import parse_qs, urlparse

DATASET = "dpe03existant"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TIMEOUT = 30


class AdemeApiHook(BaseHook):
    conn_name_attr = "ademe_conn_id"
    default_conn_name = "ademe_api"

    def __init__(self, ademe_conn_id: str = default_conn_name, timeout: int = DEFAULT_TIMEOUT) -> None:
        super().__init__()
        self.ademe_conn_id = ademe_conn_id
        self.timeout = timeout

    def _base_url(self) -> str:
        conn = self.get_connection(self.ademe_conn_id)
        schema = (conn.extra_dejson.get("schema") or "https").rstrip(":/")
        return f"{schema}://{conn.host}/data-fair/api/v1/datasets/{DATASET}"

    def _session(self) -> requests.Session:
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _build_qs(self, code_postal: str | None = None, code_insee: str | None = None) -> str | None:
        filters: list[str] = []
        if code_postal:
            filters.append(f'code_postal_ban:"{code_postal}"')
        if code_insee:
            filters.append(f'code_insee_ban:"{code_insee}"')
        return " AND ".join(filters) or None

    def _next_after(self, payload: dict) -> str | None:
        next_url = payload.get("next")
        if not next_url:
            return None
        parsed = urlparse(next_url)
        after = parse_qs(parsed.query).get("after")
        return after[0] if after else None

    def get_dpe(
        self,
        code_postal: str | None = None,
        code_insee: str | None = None,
        size: int = DEFAULT_PAGE_SIZE,
        max_pages: int | None = None,
    ) -> list[dict]:
        """Return DPE rows from ADEME, following cursor pagination."""
        url = f"{self._base_url()}/lines"
        params: dict[str, int | str] = {"size": size}
        qs = self._build_qs(code_postal=code_postal, code_insee=code_insee)
        if qs:
            params["qs"] = qs

        rows: list[dict] = []
        after: str | None = None
        page = 0

        with self._session() as session:
            while True:
                page += 1
                if after:
                    params["after"] = after

                response = session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                page_rows = payload.get("results", [])
                rows.extend(page_rows)

                after = self._next_after(payload)
                if not after or not page_rows or (max_pages is not None and page >= max_pages):
                    break

        return rows
