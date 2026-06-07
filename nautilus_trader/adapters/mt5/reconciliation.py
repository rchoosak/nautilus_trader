# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Reconciliation mapping helpers for the MetaTrader 5 adapter.
"""

from __future__ import annotations

import json
from typing import Any


MT5_RECONCILIATION_VERSION = 1


def empty_reconciliation_payload() -> dict[str, Any]:
    """
    Return an empty MT5 reconciliation mapping payload.
    """
    return {
        "version": MT5_RECONCILIATION_VERSION,
        "comments": {},
        "venue_orders": {},
        "deals": {},
        "clients": {},
    }


def decode_reconciliation_payload(raw: bytes | None) -> dict[str, Any]:
    """
    Decode a reconciliation payload from cache bytes.
    """
    if raw is None:
        return empty_reconciliation_payload()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        return empty_reconciliation_payload()
    return normalize_reconciliation_payload(payload)


def encode_reconciliation_payload(payload: dict[str, Any]) -> bytes:
    """
    Encode a reconciliation payload for the cache.
    """
    return json.dumps(
        normalize_reconciliation_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def normalize_reconciliation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Ensure a reconciliation payload has the expected top-level shape.
    """
    normalized = empty_reconciliation_payload()
    normalized["version"] = payload.get("version", MT5_RECONCILIATION_VERSION)
    for key in ("comments", "venue_orders", "deals", "clients"):
        value = payload.get(key)
        if isinstance(value, dict):
            normalized[key] = {str(k): v for k, v in value.items()}
    return normalized


def register_order_mapping(
    payload: dict[str, Any],
    *,
    client_order_id: str,
    comment: str | None = None,
    venue_order_id: str | int | None = None,
    deal_ticket: str | int | None = None,
    position_id: str | int | None = None,
    ts_event: int | None = None,
) -> dict[str, Any]:
    """
    Register or update an MT5 order mapping in the given payload.
    """
    payload = normalize_reconciliation_payload(payload)
    client_order_id = str(client_order_id)
    record = dict(payload["clients"].get(client_order_id) or {})

    if comment:
        comment = str(comment)
        payload["comments"][comment] = client_order_id
        record["comment"] = comment

    if venue_order_id:
        venue_order_id = str(venue_order_id)
        payload["venue_orders"][venue_order_id] = client_order_id
        record["venue_order_id"] = venue_order_id

    if deal_ticket:
        deal_ticket = str(deal_ticket)
        payload["deals"][deal_ticket] = client_order_id
        record["deal_ticket"] = deal_ticket

    if position_id:
        record["position_id"] = str(position_id)

    if ts_event is not None:
        record["ts_event"] = int(ts_event)

    payload["clients"][client_order_id] = record
    return payload


def client_order_id_from_comment(payload: dict[str, Any], comment: str | None) -> str | None:
    """
    Return the mapped client order ID for an MT5 comment.
    """
    if not comment:
        return None
    payload = normalize_reconciliation_payload(payload)
    value = payload["comments"].get(str(comment))
    return str(value) if value is not None else None


def client_order_id_from_venue_order_id(payload: dict[str, Any], venue_order_id: str | int | None) -> str | None:
    """
    Return the mapped client order ID for an MT5 order ticket.
    """
    if not venue_order_id:
        return None
    payload = normalize_reconciliation_payload(payload)
    value = payload["venue_orders"].get(str(venue_order_id))
    return str(value) if value is not None else None


def client_order_id_from_deal_ticket(payload: dict[str, Any], deal_ticket: str | int | None) -> str | None:
    """
    Return the mapped client order ID for an MT5 deal ticket.
    """
    if not deal_ticket:
        return None
    payload = normalize_reconciliation_payload(payload)
    value = payload["deals"].get(str(deal_ticket))
    return str(value) if value is not None else None


def comment_mappings(payload: dict[str, Any]) -> dict[str, str]:
    """
    Return comment-to-client-order-id mappings from a payload.
    """
    payload = normalize_reconciliation_payload(payload)
    return {str(comment): str(client_order_id) for comment, client_order_id in payload["comments"].items()}


def venue_order_mappings(payload: dict[str, Any]) -> dict[str, str]:
    """
    Return MT5 order-ticket-to-client-order-id mappings from a payload.
    """
    payload = normalize_reconciliation_payload(payload)
    return {str(ticket): str(client_order_id) for ticket, client_order_id in payload["venue_orders"].items()}
