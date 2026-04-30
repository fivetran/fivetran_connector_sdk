"""URL safety tests — path-segment encoding for the vPIC make.

Hazard #5 sibling (api-profiling.md): Best Buy PR #572 surfaced a class of
bugs where user-supplied (or AI-recommended) values are interpolated raw into
URL **paths**, breaking the URL when the value contains spaces, parens, or
URL-reserved characters.

NHTSA vPIC `GetModelsForMake/{make}` is the same shape:

    url = f"{__BASE_URL_VPIC}/GetModelsForMake/{make}"

Where `make` flows from:
- The user's `seed_make` configuration (uncontrolled string).
- The Cortex agent's recommended-vehicle output (LLM-generated string,
  uncontrolled).

Without `urllib.parse.quote(make, safe="")`, values like "Mercedes-Benz" with
a slash (`Mercedes/Benz`) or "Smart Car" with a space break the URL. The
connector should encode the make path segment.

These tests are EXPECTED TO FAIL RED against the current connector. The fix
is one line: `make = urllib.parse.quote(make, safe="")` before the f-string.
"""

from unittest.mock import MagicMock

import connector


def _captured_url_session(captured_urls):
    """Build a mock session whose .get records the URL it was called with."""
    session = MagicMock()

    def fake_get(url, params=None, headers=None, timeout=None):
        captured_urls.append({"url": url, "params": params})
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"Results": []}
        resp.raise_for_status.return_value = None
        return resp

    session.get = fake_get
    session.headers = MagicMock()
    return session


class TestVpicMakeUrlEncoding:
    def test_space_in_make_is_encoded(self):
        """A make like 'Land Rover' must be URL-encoded so the request URL is
        well-formed (`/GetModelsForMake/Land%20Rover`)."""
        captured = []
        session = _captured_url_session(captured)
        connector.fetch_vehicle_specs(session, "Land Rover")
        assert len(captured) == 1
        url = captured[0]["url"]
        assert (
            "Land%20Rover" in url or "Land+Rover" in url
        ), f"URL must encode space in make path segment, got: {url}"
        assert " " not in url.split("?")[0], f"Raw space in URL path is invalid HTTP, got: {url}"

    def test_slash_in_make_is_encoded(self):
        """A make containing '/' (e.g., 'Mercedes/Benz') must be encoded —
        otherwise it injects a path segment and changes the endpoint route."""
        captured = []
        session = _captured_url_session(captured)
        connector.fetch_vehicle_specs(session, "Mercedes/Benz")
        url = captured[0]["url"].split("?")[0]
        # The slash must NOT appear unencoded in the make portion.
        # We expect /GetModelsForMake/Mercedes%2FBenz, NOT /GetModelsForMake/Mercedes/Benz.
        assert url.endswith("/GetModelsForMake/Mercedes%2FBenz") or url.endswith(
            "/GetModelsForMake/Mercedes%2fBenz"
        ), f"'/' in make must be percent-encoded as %2F, got: {url}"

    def test_question_mark_in_make_is_encoded(self):
        """A '?' in the make would otherwise inject a query string boundary."""
        captured = []
        session = _captured_url_session(captured)
        connector.fetch_vehicle_specs(session, "Brand?Name")
        url = captured[0]["url"]
        # The '?' in the make should be encoded as %3F in the path; only the
        # format=json query separator should appear as a real '?'. So the count
        # of unencoded '?' characters should be at most 1.
        assert (
            url.count("?") <= 1
        ), f"'?' in make must be encoded (got {url.count('?')} unencoded ?s in: {url})"
