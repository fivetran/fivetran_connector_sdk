"""Rillet Connector for Fivetran Connector SDK.

This connector fetches frequently used resources from the Rillet API and upserts
records into destination tables. It uses keyset pagination, incremental sync,
and checkpointing to handle large datasets safely.

See https://docs.api.rillet.com/docs/getting-started
and https://docs.api.rillet.com/docs/webhooks
"""

import json
import time
from typing import Dict

import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__DEFAULT_BASE_URL = "https://api.rillet.com"
__DEFAULT_API_VERSION = "3"
__MAX_RETRIES = 5
__BACKOFF_BASE = 2
__PAGE_LIMIT = 100
__CHECKPOINT_INTERVAL = 200

SYNC_COLLECTIONS = [
    {
        "endpoint": "/accounts",
        "response_key": "accounts",
        "table": "account",
        "cursor_key": "accounts_cursor",
        "last_updated_key": "accounts_last_updated_at",
        "supports_pagination": False,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/subsidiaries",
        "response_key": "subsidiaries",
        "table": "subsidiary",
        "cursor_key": "subsidiaries_cursor",
        "last_updated_key": "subsidiaries_last_updated_at",
        "supports_pagination": False,
        "supports_updated_gt": False,
    },
    {
        "endpoint": "/products",
        "response_key": "products",
        "table": "product",
        "cursor_key": "products_cursor",
        "last_updated_key": "products_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": False,
    },
    {
        "endpoint": "/customers",
        "response_key": "customers",
        "table": "customer",
        "cursor_key": "customers_cursor",
        "last_updated_key": "customers_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/contracts",
        "response_key": "contracts",
        "table": "contract",
        "cursor_key": "contracts_cursor",
        "last_updated_key": "contracts_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": False,
    },
    {
        "endpoint": "/invoices",
        "response_key": "invoices",
        "table": "invoice",
        "cursor_key": "invoices_cursor",
        "last_updated_key": "invoices_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/invoice-payments",
        "response_key": "payments",
        "table": "invoice_payment",
        "cursor_key": "invoice_payments_cursor",
        "last_updated_key": "invoice_payments_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/credit-memos",
        "response_key": "credit_memos",
        "table": "credit_memo",
        "cursor_key": "credit_memos_cursor",
        "last_updated_key": "credit_memos_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/vendors",
        "response_key": "vendors",
        "table": "vendor",
        "cursor_key": "vendors_cursor",
        "last_updated_key": "vendors_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/vendor-credits",
        "response_key": "vendor_credits",
        "table": "vendor_credit",
        "cursor_key": "vendor_credits_cursor",
        "last_updated_key": "vendor_credits_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
        "optional": True,
    },
    {
        "endpoint": "/bills",
        "response_key": "bills",
        "table": "bill",
        "cursor_key": "bills_cursor",
        "last_updated_key": "bills_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/charges",
        "response_key": "charges",
        "table": "charge",
        "cursor_key": "charges_cursor",
        "last_updated_key": "charges_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/reimbursements",
        "response_key": "reimbursements",
        "table": "reimbursement",
        "cursor_key": "reimbursements_cursor",
        "last_updated_key": "reimbursements_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/journal-entries",
        "response_key": "journal_entries",
        "table": "journal_entry",
        "cursor_key": "journal_entries_cursor",
        "last_updated_key": "journal_entries_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/bank-accounts",
        "response_key": "accounts",
        "table": "bank_account",
        "cursor_key": "bank_accounts_cursor",
        "last_updated_key": "bank_accounts_last_updated_at",
        "supports_pagination": False,
        "supports_updated_gt": False,
    },
    {
        "endpoint": "/bank-transactions",
        "response_key": "bank_transactions",
        "table": "bank_transaction",
        "cursor_key": "bank_transactions_cursor",
        "last_updated_key": "bank_transactions_last_updated_at",
        "supports_pagination": True,
        "supports_updated_gt": True,
    },
    {
        "endpoint": "/tax-rates",
        "response_key": "tax_rates",
        "table": "tax_rate",
        "cursor_key": "tax_rates_cursor",
        "last_updated_key": "tax_rates_last_updated_at",
        "supports_pagination": False,
        "supports_updated_gt": False,
    },
    {
        "endpoint": "/fields",
        "response_key": "fields",
        "table": "field",
        "cursor_key": "fields_cursor",
        "last_updated_key": "fields_last_updated_at",
        "supports_pagination": False,
        "supports_updated_gt": True,
    },
]


def validate_configuration(configuration: Dict):
    """Validate required and optional connector configuration."""
    required_keys = ["api_key"]
    for key in required_keys:
        if key not in configuration or not configuration.get(key):
            raise ValueError(f"Missing required configuration value: {key}")

    base_url = configuration.get("base_url", __DEFAULT_BASE_URL)
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty string")

    api_version = configuration.get("api_version", __DEFAULT_API_VERSION)
    if not isinstance(api_version, (str, int)) or not str(api_version).strip():
        raise ValueError("api_version must be a non-empty string or number")


def schema(configuration: Dict):
    """Defines destination tables for the connector."""
    return [{"table": collection["table"], "primary_key": ["id"]} for collection in SYNC_COLLECTIONS]


def _make_headers(configuration: Dict) -> Dict:
    return {
        "Authorization": f"Bearer {configuration['api_key']}",
        "accept": "application/json",
        "X-Rillet-API-Version": str(configuration.get("api_version", __DEFAULT_API_VERSION)),
    }


def _make_request(url: str, headers: Dict, params: Dict, optional: bool = False):
    last_exception = None
    for attempt in range(1, __MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "1"))
                sleep_seconds = max(retry_after, __BACKOFF_BASE ** attempt)
                log.warning(f"Rate limit received, sleeping {sleep_seconds}s (attempt {attempt})")
                time.sleep(sleep_seconds)
                continue

            if 500 <= response.status_code < 600:
                sleep_seconds = __BACKOFF_BASE ** attempt
                log.warning(
                    f"Server error {response.status_code}. Retry in {sleep_seconds}s (attempt {attempt})"
                )
                time.sleep(sleep_seconds)
                continue

            if 400 <= response.status_code < 500:
                if optional and response.status_code == 404:
                    log.warning(f"Optional endpoint returned 404, skipping: {url}")
                    return None
                # Fail fast for client errors that require config or request changes
                response.raise_for_status()

            return response.json()

        except requests.RequestException as e:
            last_exception = e
            if attempt == __MAX_RETRIES:
                raise RuntimeError(f"Failed to request {url}: {e}")
            sleep_seconds = __BACKOFF_BASE ** attempt
            log.warning(f"Request exception: {e}. Retry in {sleep_seconds}s (attempt {attempt})")
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to get API response for {url}") from last_exception


def _get_collection_items(
    configuration: Dict,
    endpoint: str,
    response_key: str,
    table_name: str,
    cursor_key: str,
    state: Dict,
    last_updated_key: str,
    supports_pagination: bool,
    supports_updated_gt: bool,
    optional: bool = False,
):
    """Fetch records from one endpoint with optional pagination and incremental updates."""
    base_url = configuration.get("base_url", __DEFAULT_BASE_URL).rstrip("/")
    url = f"{base_url}{endpoint}"
    headers = _make_headers(configuration)

    cursor = state.get(cursor_key) if supports_pagination else None
    last_synced_at = state.get(last_updated_key) if supports_updated_gt else None
    retrieved = 0
    highest_updated = last_synced_at

    while True:
        params = {}
        if supports_pagination:
            params["limit"] = __PAGE_LIMIT
            if cursor:
                params["cursor"] = cursor
        if supports_updated_gt and not cursor and last_synced_at:
            params["updated.gt"] = last_synced_at

        log.info(
            f"Fetching {endpoint} table={table_name} cursor={cursor} updated.gt={last_synced_at}"
        )
        payload = _make_request(url=url, headers=headers, params=params, optional=optional)

        if payload is None:
            log.info(f"Skipping optional endpoint because it is not available: {endpoint}")
            return
        if not payload:
            break

        items = payload.get(response_key) if isinstance(payload, dict) else None
        if items is None:
            raise RuntimeError(f"Unexpected response shape for {endpoint}: missing '{response_key}'")

        if not isinstance(items, list) or len(items) == 0:
            break

        for item in items:
            updated_at = item.get("updated_at") or item.get("created_at")
            if last_synced_at and updated_at and updated_at <= last_synced_at:
                continue

            op.upsert(table=table_name, data=item)
            retrieved += 1

            if updated_at and (highest_updated is None or updated_at > highest_updated):
                highest_updated = updated_at

            if retrieved % __CHECKPOINT_INTERVAL == 0:
                if supports_pagination:
                    state[cursor_key] = cursor
                if supports_updated_gt and highest_updated:
                    state[last_updated_key] = highest_updated
                op.checkpoint(state)

        if not supports_pagination:
            break

        pagination = payload.get("pagination", {})
        next_cursor = pagination.get("next_cursor")
        if not next_cursor:
            break

        cursor = next_cursor

    if supports_pagination:
        state[cursor_key] = cursor
    if supports_updated_gt and highest_updated:
        state[last_updated_key] = highest_updated

    op.checkpoint(state)
    log.info(f"Fetched {retrieved} records for {endpoint}")


def update(configuration: Dict, state: Dict):
    """Main sync function called by Fivetran during each run."""
    log.warning("Example: Source Connector : Rillet API Connector")

    validate_configuration(configuration)

    try:
        for item in SYNC_COLLECTIONS:
            _get_collection_items(
                configuration=configuration,
                endpoint=item["endpoint"],
                response_key=item["response_key"],
                table_name=item["table"],
                cursor_key=item["cursor_key"],
                state=state,
                last_updated_key=item["last_updated_key"],
                supports_pagination=item["supports_pagination"],
                supports_updated_gt=item["supports_updated_gt"],
                optional=item.get("optional", False),
            )

    except Exception as e:
        log.severe(f"Failed to sync Rillet data: {e}")
        raise


connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
