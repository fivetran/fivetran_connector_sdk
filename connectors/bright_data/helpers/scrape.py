"""Bright Data Web Scraper helper functions."""

import time
from typing import Any, Dict, List, Optional, Union

from brightdata import bdclient
from fivetran_connector_sdk import Logging as log


def perform_scrape(
    client: bdclient,
    url: Union[str, List[str]],
    country: Optional[str] = None,
    data_format: Optional[str] = None,
    format_param: Optional[str] = "json",
    method: Optional[str] = None,
    async_request: bool = True,
    max_poll_attempts: int = 60,
    poll_interval: int = 5,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Scrape URLs using Bright Data's Web Scraper SDK.
    """
    if not url:
        raise ValueError("URL cannot be empty")

    if not isinstance(url, (str, list)):
        raise TypeError("URL must be a string or list of strings")

    try:
        scrape_params: Dict[str, Any] = {}

        if country:
            scrape_params["country"] = country.lower()
        if data_format:
            scrape_params["data_format"] = data_format
        if format_param:
            scrape_params["format"] = format_param
        if method:
            scrape_params["method"] = method
        if async_request:
            scrape_params["async_request"] = True

        url_count = len(url) if isinstance(url, list) else 1
        request_type = "async" if async_request else "sync"
        log.info(
            f"Executing Bright Data scrape ({request_type}) for {url_count} URL"
            f"{'s' if url_count > 1 else ''}"
        )

        results = client.scrape(url, **scrape_params)

        if async_request:
            snapshot_ids = _extract_snapshot_ids(results, url_count)

            if snapshot_ids:
                valid_snapshot_ids = [
                    sid
                    for sid in snapshot_ids
                    if isinstance(sid, str)
                    and len(sid) < 200
                    and not sid.strip().startswith("<")
                ]

                if valid_snapshot_ids:
                    results = _poll_snapshots(
                        client, valid_snapshot_ids, max_poll_attempts, poll_interval
                    )

        if isinstance(results, list) and len(results) > 0:
            parsed_results: List[Dict[str, Any]] = []
            for result in results:
                if not result or (isinstance(result, str) and len(result.strip()) == 0):
                    continue

                parsed = client.parse_content(
                    result,
                    extract_text=True,
                    extract_links=True,
                    extract_images=True,
                )
                if isinstance(parsed, list):
                    parsed_results.extend(parsed)
                else:
                    parsed_results.append(parsed)
        else:
            parsed_results = client.parse_content(
                results,
                extract_text=True,
                extract_links=True,
                extract_images=True,
            )
            if not isinstance(parsed_results, list):
                parsed_results = [parsed_results]

        result_count = len(parsed_results)
        log.info(f"Scrape completed successfully. Retrieved {result_count} result(s)")

        return parsed_results

    except ValueError as e:
        log.info(f"Validation error in Bright Data scrape: {str(e)}")
        raise
    except TypeError as e:
        log.info(f"Type error in Bright Data scrape: {str(e)}")
        raise
    except Exception as e:
        log.info(f"Error performing Bright Data scrape: {str(e)}")
        raise


def _extract_snapshot_ids(
    results: Union[Dict[str, Any], List[Dict[str, Any]], str], _url_count: int
) -> List[str]:
    snapshot_ids: List[str] = []

    def is_valid_snapshot_id(sid: str) -> bool:
        if not isinstance(sid, str) or not sid.strip():
            return False
        if len(sid) > 200:
            return False
        if sid.strip().startswith("<") or sid.strip().startswith("<!"):
            return False
        if "<html" in sid.lower() or "<body" in sid.lower():
            return False
        return True

    if isinstance(results, list):
        for result in results:
            if isinstance(result, dict):
                snapshot_id = result.get("snapshot_id")
                if snapshot_id and is_valid_snapshot_id(str(snapshot_id)):
                    snapshot_ids.append(str(snapshot_id))
            elif isinstance(result, str):
                if is_valid_snapshot_id(result):
                    snapshot_ids.append(result)
    elif isinstance(results, dict):
        snapshot_id = results.get("snapshot_id")
        if snapshot_id and is_valid_snapshot_id(str(snapshot_id)):
            snapshot_ids.append(str(snapshot_id))
    elif isinstance(results, str):
        if is_valid_snapshot_id(results):
            snapshot_ids.append(results)

    return snapshot_ids


def _poll_snapshots(
    client: bdclient,
    snapshot_ids: List[str],
    max_attempts: int = 20,
    poll_interval: int = 5,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    results: List[Any] = []
    completed_snapshots = set()
    failed_snapshots: List[str] = []

    for attempt in range(max_attempts):
        if len(completed_snapshots) == len(snapshot_ids):
            break

        for snapshot_id in snapshot_ids:
            if snapshot_id in completed_snapshots or snapshot_id in failed_snapshots:
                continue

            try:
                snapshot_response = client.download_snapshot(snapshot_id)

                status = None
                snapshot_data = None

                if isinstance(snapshot_response, dict):
                    status = snapshot_response.get("status", "").lower()
                    if status == "ready":
                        snapshot_data = (
                            snapshot_response.get("data") or snapshot_response
                        )
                    elif status == "failed":
                        error_msg = snapshot_response.get("error", "Unknown error")
                        failed_snapshots.append(snapshot_id)
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... status: failed - {error_msg}"
                        )
                        continue
                    elif status == "running":
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... status: running (attempt {attempt + 1}/{max_attempts})"
                        )
                        continue
                elif snapshot_response:
                    snapshot_data = snapshot_response
                    status = "ready"

                if snapshot_data:
                    results.append(snapshot_data)
                    completed_snapshots.add(snapshot_id)
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: ready - completed ({len(completed_snapshots)}/{len(snapshot_ids)})"
                    )
                else:
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... still processing (attempt {attempt + 1}/{max_attempts})"
                    )

            except (ValueError, RuntimeError, KeyError) as e:
                error_msg = str(e).lower()

                if "failed" in error_msg:
                    failed_snapshots.append(snapshot_id)
                    log.info(f"Snapshot {snapshot_id[:8]}... failed: {str(e)}")
                elif (
                    "not ready" in error_msg
                    or "not found" in error_msg
                    or "pending" in error_msg
                    or "running" in error_msg
                ):
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: running (attempt {attempt + 1}/{max_attempts})"
                    )
                else:
                    log.info(f"Error polling snapshot {snapshot_id[:8]}...: {str(e)}")

        if len(completed_snapshots) < len(snapshot_ids) and attempt < max_attempts - 1:
            time.sleep(poll_interval)

    if failed_snapshots:
        failed_list = ", ".join([sid[:8] + "..." for sid in failed_snapshots])
        raise RuntimeError(f"Snapshot(s) failed: {failed_list}")

    if len(completed_snapshots) < len(snapshot_ids):
        incomplete = len(snapshot_ids) - len(completed_snapshots)
        raise RuntimeError(
            f"Timeout: {incomplete} snapshot(s) did not complete after {max_attempts} attempts"
        )

    if len(results) == 1:
        return results[0]
    return results

