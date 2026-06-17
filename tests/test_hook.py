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
