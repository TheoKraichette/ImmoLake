"""Tests unitaires du Custom Hook ADEME (mock de requests, pas d'appel réseau réel)."""
from unittest.mock import patch, MagicMock

from hooks.ademe_api_hook import AdemeApiHook


@patch("hooks.ademe_api_hook.requests.get")
@patch.object(AdemeApiHook, "get_connection")
def test_get_dpe_parses_results(mock_conn, mock_get):
    # Connexion factice : host + schema https
    conn = MagicMock(host="data.ademe.fr")
    conn.extra_dejson = {"schema": "https"}
    mock_conn.return_value = conn

    # Réponse API mockée
    resp = MagicMock()
    resp.json.return_value = {"results": [{"numero_dpe": "X"}, {"numero_dpe": "Y"}]}
    resp.raise_for_status.return_value = None
    mock_get.return_value = resp

    rows = AdemeApiHook().get_dpe(code_postal="75001", size=10)

    assert len(rows) == 2
    assert rows[0]["numero_dpe"] == "X"
    # On appelle bien l'URL https du dataset
    called_url = mock_get.call_args.args[0]
    assert called_url.startswith("https://data.ademe.fr/")
