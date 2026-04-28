"""Lucene escape + URL encoding tests — locks in the fix for Sahil/Copilot
finding #3.

Bug: search_drug was interpolated directly into a Lucene query and the
URL with no escaping. Drug names with spaces broke the URL; characters
like `+`, `:`, `&`, `(`, `)` either broke the request or could alter the
HTTP query parameters (e.g., `&` injecting a new param).

Fix: a dedicated _build_search_query() helper that
  (a) Lucene-escapes reserved characters (`+ - && || ! ( ) { } [ ] ^ " ~
      * ? : \\ /`) by prefixing them with backslash
  (b) URL-encodes the resulting value with safe='' so spaces, +, &, :,
      and the escape backslashes are all percent-encoded

This is a NEW bug class for the retrospective (Lucene injection) — no
prior PR surfaced it.
"""

import urllib.parse
from datetime import datetime

import connector


class TestBuildSearchQueryNoDrug:
    """Without a drug filter, only the date range appears."""

    def test_date_range_only(self):
        query = connector._build_search_query("20260101", "20260201", None)
        assert "receivedate:[20260101+TO+20260201]" in query
        # No medicinalproduct clause when search_drug is None/empty.
        assert "medicinalproduct" not in query

    def test_empty_drug_treated_as_no_filter(self):
        query = connector._build_search_query("20260101", "20260201", "")
        assert "medicinalproduct" not in query


class TestBuildSearchQueryEscapesLuceneChars:
    """Each Lucene-reserved character must be backslash-escaped before URL
    encoding. After encoding, the URL must be valid."""

    def test_space_becomes_url_encoded(self):
        query = connector._build_search_query("20260101", "20260201", "foo bar")
        assert " " not in query, f"raw space leaked into query: {query!r}"
        assert "%20" in query

    def test_plus_is_escaped_and_encoded(self):
        # `+` is Lucene reserved (means must-include) AND is URL-encoded
        # by Lucene to mean space. Escape with backslash, then URL-encode.
        query = connector._build_search_query("20260101", "20260201", "drug+other")
        # The escape backslash becomes %5C, the literal + becomes %2B.
        assert "%5C%2B" in query, f"`+` must be backslash-escaped and URL-encoded; got {query!r}"

    def test_colon_is_escaped_and_encoded(self):
        # `:` is Lucene reserved (field separator) — must be escaped to
        # avoid being interpreted as starting a new field clause.
        query = connector._build_search_query("20260101", "20260201", "AB:CD")
        assert "%5C%3A" in query, f"`:` must be backslash-escaped and URL-encoded; got {query!r}"

    def test_ampersand_is_url_encoded(self):
        # `&` is not Lucene-reserved but IS URL-reserved; if not encoded
        # it would inject a new query parameter into the URL.
        query = connector._build_search_query("20260101", "20260201", "foo&bar")
        assert "%26" in query, f"`&` must be URL-encoded; got {query!r}"

    def test_parens_and_brackets_escaped(self):
        for char in ("(", ")", "{", "}", "[", "]"):
            query = connector._build_search_query("20260101", "20260201", f"x{char}y")
            assert (
                char not in query.split("medicinalproduct:")[1]
            ), f"raw {char!r} leaked into Lucene value of {query!r}"


class TestSearchUrlIsValid:
    """The full URL the connector hands to requests.get must be parseable
    and contain only ASCII-safe characters in the query string."""

    def test_url_with_special_drug_name_is_valid(self, monkeypatch):
        captured_urls = []

        def fake_get(url, params=None, timeout=None):
            captured_urls.append(url)
            from unittest.mock import MagicMock

            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"results": [], "meta": {"results": {"total": 0}}}
            resp.raise_for_status.return_value = None
            return resp

        from unittest.mock import MagicMock

        session = MagicMock()
        session.get.side_effect = fake_get
        monkeypatch.setattr(connector.time, "sleep", lambda *_: None)

        # Build the query and call the actual fetch_adverse_events to see
        # the URL it constructs.
        query = connector._build_search_query("20260101", "20260201", "foo+bar baz")
        connector.fetch_adverse_events(session, query, 10, 0)

        assert len(captured_urls) == 1
        url = captured_urls[0]
        # Confirm the URL parses cleanly.
        parsed = urllib.parse.urlparse(url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "api.fda.gov"
        # Critical: no raw spaces in the URL (would break the request).
        assert " " not in url, f"raw space in URL: {url!r}"
        # No raw `+` should appear inside the medicinalproduct value.
        # The `+TO+` and `+AND+` separators may include `+` but the user
        # value `foo+bar` must be backslash-escaped + URL-encoded.
        assert "%5C%2B" in url, f"user `+` not escaped in URL: {url!r}"


class TestEndToEndDoesNotInjectLuceneClause:
    """Verify a malicious search_drug like `aspirin OR drug:badmetal`
    cannot inject a new Lucene field clause."""

    def test_injection_attempt_is_neutralized(self):
        attack = 'aspirin" OR drug:badmetal'
        query = connector._build_search_query("20260101", "20260201", attack)
        # The injected `:` must be escaped, not interpreted as a field
        # separator.
        assert "%5C%3A" in query, "`:` from injection attempt was not escaped"
        # The double-quote should also be escaped.
        assert "%5C%22" in query, '`"` from injection attempt was not escaped'


def _datetime_iso():
    return datetime.now().isoformat()
