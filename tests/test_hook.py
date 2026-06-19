"""Unit tests for the ADEME hook."""
from unittest.mock import MagicMock, patch

from hooks.ademe_api_hook import AdemeApiHook


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        return self.responses.pop(0)


def fake_response(payload):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


@patch.object(AdemeApiHook, "get_connection")
def test_get_dpe_parses_results(mock_conn):
    conn = MagicMock(host="data.ademe.fr")
    conn.extra_dejson = {"schema": "https"}
    mock_conn.return_value = conn

    session = FakeSession([fake_response({"results": [{"numero_dpe": "X"}, {"numero_dpe": "Y"}]})])
    hook = AdemeApiHook()
    hook._session = MagicMock(return_value=session)

    rows = hook.get_dpe(code_postal="75001", size=10)

    assert len(rows) == 2
    assert rows[0]["numero_dpe"] == "X"
    assert session.calls[0]["url"].startswith("https://data.ademe.fr/")
    assert session.calls[0]["params"]["qs"] == 'code_postal_ban:"75001"'
    assert session.calls[0]["params"]["size"] == 10


@patch.object(AdemeApiHook, "get_connection")
def test_get_dpe_follows_cursor_pagination(mock_conn):
    conn = MagicMock(host="data.ademe.fr")
    conn.extra_dejson = {"schema": "https"}
    mock_conn.return_value = conn

    session = FakeSession(
        [
            fake_response(
                {
                    "results": [{"numero_dpe": "X"}],
                    "next": (
                        "https://data.ademe.fr/data-fair/api/v1/datasets/"
                        "dpe03existant/lines?size=1&after=cursor-2"
                    ),
                }
            ),
            fake_response({"results": [{"numero_dpe": "Y"}]}),
        ]
    )
    hook = AdemeApiHook()
    hook._session = MagicMock(return_value=session)

    rows = hook.get_dpe(code_insee="75056", size=1)

    assert [row["numero_dpe"] for row in rows] == ["X", "Y"]
    assert "after" not in session.calls[0]["params"]
    assert session.calls[1]["params"]["after"] == "cursor-2"
    assert session.calls[0]["params"]["qs"] == 'code_insee_ban:"75056"'


@patch.object(AdemeApiHook, "get_connection")
def test_iter_dpe_yields_pages_lazily(mock_conn):
    """Le générateur rend une page à la fois (pas d'accumulation globale en mémoire)."""
    conn = MagicMock(host="data.ademe.fr")
    conn.extra_dejson = {"schema": "https"}
    mock_conn.return_value = conn

    base = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
    session = FakeSession(
        [
            fake_response({"results": [{"numero_dpe": "A"}], "next": f"{base}?after=c2"}),
            fake_response({"results": [{"numero_dpe": "B"}], "next": f"{base}?after=c3"}),
            fake_response({"results": [{"numero_dpe": "C"}]}),
        ]
    )
    hook = AdemeApiHook()
    hook._session = MagicMock(return_value=session)

    pages = hook.iter_dpe(departement="33", size=1)
    first = next(pages)

    # Filtre par département + paresse : une seule page récupérée à ce stade.
    assert first == [{"numero_dpe": "A"}]
    assert session.calls[0]["params"]["qs"] == 'code_departement_ban:"33"'
    assert len(session.calls) == 1

    rest = list(pages)
    assert [page[0]["numero_dpe"] for page in rest] == ["B", "C"]
    assert len(session.calls) == 3
    assert session.calls[1]["params"]["after"] == "c2"


@patch.object(AdemeApiHook, "get_connection")
def test_iter_dpe_restricts_columns_via_select(mock_conn):
    """`select` borne les colonnes demandées à l'API (allège pages réseau + Parquet raw)."""
    conn = MagicMock(host="data.ademe.fr")
    conn.extra_dejson = {"schema": "https"}
    mock_conn.return_value = conn

    session = FakeSession([fake_response({"results": [{"numero_dpe": "A"}]})])
    hook = AdemeApiHook()
    hook._session = MagicMock(return_value=session)

    next(hook.iter_dpe(departement="33", size=10))

    select = session.calls[0]["params"]["select"]
    # Champs pertinents (DPE + GES + coût + année) présents ; pas tout le dataset (230 colonnes).
    for field in ("etiquette_dpe", "etiquette_ges", "annee_construction", "cout_total_5_usages"):
        assert field in select
