"""Hook API DPE ADEME (connexion Airflow `ademe_api`).

Endpoint : GET https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines
"""
from __future__ import annotations

import time
from collections.abc import Iterator

from airflow.sdk.bases.hook import BaseHook
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import parse_qs, urlparse

DATASET = "dpe03existant"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TIMEOUT = 60
PAGE_RETRIES = 5  # pagination profonde ADEME : retente une page sur timeout/erreur réseau transitoire

# Le dataset DPE compte ~230 colonnes ; on n'en exploite qu'une poignée. Passer `select` à l'API
# (paramètre data-fair) allège drastiquement les pages (réseau + Parquet raw) et permet de viser plus
# de départements. La liste est CANONIQUE : les `silver_*` lisent exactement ces noms de champs.
DPE_SELECT_FIELDS = (
    "numero_dpe",
    "date_etablissement_dpe",
    "code_insee_ban",
    "code_postal_ban",
    "nom_commune_ban",
    "type_batiment",
    "surface_habitable_logement",
    "annee_construction",
    "etiquette_dpe",
    "etiquette_ges",
    "conso_5_usages_par_m2_ep",
    "emission_ges_5_usages_par_m2",
    "cout_total_5_usages",
    "type_energie_principale_chauffage",
)


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

    def _build_qs(
        self,
        departement: str | None = None,
        code_postal: str | None = None,
        code_insee: str | None = None,
    ) -> str | None:
        filters: list[str] = []
        if departement:
            filters.append(f'code_departement_ban:"{departement}"')
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

    def iter_dpe(
        self,
        *,
        departement: str | None = None,
        code_postal: str | None = None,
        code_insee: str | None = None,
        size: int = DEFAULT_PAGE_SIZE,
        max_pages: int | None = None,
        select: tuple[str, ...] | None = DPE_SELECT_FIELDS,
    ) -> Iterator[list[dict]]:
        """Yield les pages DPE (≤`size` lignes) une à une, en suivant le curseur `after`.

        Générateur : ne conserve jamais l'ensemble des résultats en mémoire (≠ `get_dpe`),
        ce qui permet d'ingérer un département entier — puis la France — sans OOM.

        `select` restreint les colonnes renvoyées par l'API (défaut : `DPE_SELECT_FIELDS`).
        Passer `None`/`()` pour récupérer toutes les colonnes du dataset.
        """
        url = f"{self._base_url()}/lines"
        params: dict[str, int | str] = {"size": size}
        if select:
            params["select"] = ",".join(select)
        qs = self._build_qs(departement=departement, code_postal=code_postal, code_insee=code_insee)
        if qs:
            params["qs"] = qs

        after: str | None = None
        page = 0
        with self._session() as session:
            while True:
                page += 1
                if after:
                    params["after"] = after

                payload = None
                for attempt in range(PAGE_RETRIES):
                    try:
                        response = session.get(url, params=params, timeout=self.timeout)
                        response.raise_for_status()
                        payload = response.json()
                        break
                    except (requests.ConnectionError, requests.Timeout):
                        if attempt == PAGE_RETRIES - 1:
                            raise
                        time.sleep(min(2 ** attempt, 15))
                page_rows = payload.get("results", [])
                if page_rows:
                    yield page_rows

                after = self._next_after(payload)
                if not after or not page_rows or (max_pages is not None and page >= max_pages):
                    break

    def get_dpe(
        self,
        code_postal: str | None = None,
        code_insee: str | None = None,
        size: int = DEFAULT_PAGE_SIZE,
        max_pages: int | None = None,
        select: tuple[str, ...] | None = DPE_SELECT_FIELDS,
    ) -> list[dict]:
        """Agrège toutes les pages en mémoire (compat). Préférer `iter_dpe` sur gros volumes."""
        rows: list[dict] = []
        for page_rows in self.iter_dpe(
            code_postal=code_postal, code_insee=code_insee, size=size, max_pages=max_pages, select=select
        ):
            rows.extend(page_rows)
        return rows
