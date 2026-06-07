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
Symbol helpers for the MetaTrader 5 adapter.
"""

from __future__ import annotations

import re

from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.model.identifiers import InstrumentId


_FX_RE = re.compile(r"^[A-Z]{6}$")


def mt5_symbol_from_instrument_id(instrument_id: InstrumentId) -> str:
    """
    Return the MT5-native symbol encoded by a Nautilus instrument ID.
    """
    return instrument_id.symbol.value.replace("/", "")


def instrument_id_from_mt5_symbol(symbol: str) -> InstrumentId:
    """
    Return a Nautilus instrument ID for an MT5-native symbol.
    """
    return InstrumentId.from_str(f"{symbol}.{MT5_VENUE.value}")


def strip_symbol_suffix(symbol: str, suffixes: list[str] | None) -> str:
    """
    Strip a configured broker suffix from ``symbol``.
    """
    if not suffixes:
        return symbol
    for suffix in suffixes:
        if suffix and symbol.endswith(suffix):
            return symbol[: -len(suffix)]
    return symbol


def infer_fx_currencies(symbol: str, suffixes: list[str] | None = None) -> tuple[str, str] | None:
    """
    Infer base and quote currency codes from a six-letter FX symbol.
    """
    normalized = strip_symbol_suffix(symbol.upper().replace("/", ""), suffixes)
    if not _FX_RE.match(normalized):
        return None
    return normalized[:3], normalized[3:]

