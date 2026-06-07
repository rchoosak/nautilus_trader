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

from nautilus_trader.adapters.mt5.reconciliation import client_order_id_from_comment
from nautilus_trader.adapters.mt5.reconciliation import client_order_id_from_deal_ticket
from nautilus_trader.adapters.mt5.reconciliation import client_order_id_from_venue_order_id
from nautilus_trader.adapters.mt5.reconciliation import comment_mappings
from nautilus_trader.adapters.mt5.reconciliation import decode_reconciliation_payload
from nautilus_trader.adapters.mt5.reconciliation import empty_reconciliation_payload
from nautilus_trader.adapters.mt5.reconciliation import encode_reconciliation_payload
from nautilus_trader.adapters.mt5.reconciliation import register_order_mapping
from nautilus_trader.adapters.mt5.reconciliation import venue_order_mappings


def test_register_order_mapping_indexes_comment_order_and_deal() -> None:
    payload = register_order_mapping(
        empty_reconciliation_payload(),
        client_order_id="O-20260606-001",
        comment="NT:abc123",
        venue_order_id=100200,
        deal_ticket=300400,
        position_id=500600,
        ts_event=123456789,
    )

    assert client_order_id_from_comment(payload, "NT:abc123") == "O-20260606-001"
    assert client_order_id_from_venue_order_id(payload, "100200") == "O-20260606-001"
    assert client_order_id_from_deal_ticket(payload, "300400") == "O-20260606-001"
    assert payload["clients"]["O-20260606-001"] == {
        "comment": "NT:abc123",
        "venue_order_id": "100200",
        "deal_ticket": "300400",
        "position_id": "500600",
        "ts_event": 123456789,
    }


def test_encode_decode_reconciliation_payload_round_trips_cache_bytes() -> None:
    payload = register_order_mapping(
        empty_reconciliation_payload(),
        client_order_id="O-20260606-002",
        comment="O-20260606-002",
        venue_order_id="700800",
    )

    decoded = decode_reconciliation_payload(encode_reconciliation_payload(payload))

    assert comment_mappings(decoded) == {"O-20260606-002": "O-20260606-002"}
    assert venue_order_mappings(decoded) == {"700800": "O-20260606-002"}


def test_register_order_mapping_updates_existing_client_record() -> None:
    payload = register_order_mapping(
        empty_reconciliation_payload(),
        client_order_id="O-20260606-003",
        comment="NT:def456",
    )
    payload = register_order_mapping(
        payload,
        client_order_id="O-20260606-003",
        venue_order_id=900100,
        deal_ticket=900101,
    )

    assert client_order_id_from_comment(payload, "NT:def456") == "O-20260606-003"
    assert client_order_id_from_venue_order_id(payload, 900100) == "O-20260606-003"
    assert client_order_id_from_deal_ticket(payload, 900101) == "O-20260606-003"
