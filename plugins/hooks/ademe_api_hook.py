"""Hook API DPE ADEME (connexion Airflow `ademe_api`).

Endpoint : GET https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines
"""
from __future__ import annotations

from airflow.hooks.base import BaseHook
import requests

DATASET = "dpe03existant"


class AdemeApiHook(BaseHook):
    conn_name_attr = "ademe_conn_id"
    default_conn_name = "ademe_api"

    def __init__(self, ademe_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.ademe_conn_id = ademe_conn_id

    def _base_url(self) -> str:
        conn = self.get_connection(self.ademe_conn_id)
        schema = (conn.extra_dejson.get("schema") or "https").rstrip(":/")
        return f"{schema}://{conn.host}/data-fair/api/v1/datasets/{DATASET}"

    def get_dpe(self, code_postal: str | None = None, size: int = 1000) -> list[dict]:
        # TODO: pagination par curseur `after` + retry sur 5xx
        url = f"{self._base_url()}/lines"
        params = {"size": size}
        if code_postal:
            params["qs"] = f'code_postal_ban:"{code_postal}"'
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("results", [])
