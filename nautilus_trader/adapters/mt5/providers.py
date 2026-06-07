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
Instrument provider for MetaTrader 5.
"""

from typing import Any

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge
from nautilus_trader.adapters.mt5.config import MT5InstrumentProviderConfig
from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.adapters.mt5.parsing import parse_instrument
from nautilus_trader.adapters.mt5.symbol import mt5_symbol_from_instrument_id
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId


class MT5InstrumentProvider(InstrumentProvider):
    """
    Provides Nautilus instrument definitions from an MT5 terminal.
    """

    def __init__(
        self,
        bridge: MT5TerminalBridge,
        config: MT5InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._bridge = bridge
        self._config = config or MT5InstrumentProviderConfig()
        self._log_warnings = self._config.log_warnings
        load_all_on_start = bool(getattr(self, "_load_all_on_start", False) or self._config.load_symbols)
        self._load_all_on_start = load_all_on_start

    async def load_all_async(self, filters: dict | None = None) -> None:
        group = filters.get("group") if filters else None
        symbols = await self._bridge.symbols_get(group=group)
        if symbols is None:
            self._log.warning(f"MT5 symbols_get returned None: {self._bridge.last_error()}")
            return

        load_symbols = set(self._config.load_symbols or [])
        for symbol_info in symbols:
            if load_symbols and symbol_info.name not in load_symbols:
                continue
            self._load_symbol_info(symbol_info)

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        if not instrument_ids:
            self._log.warning("No instrument IDs given for loading")
            return

        for instrument_id in instrument_ids:
            PyCondition.equal(instrument_id.venue, MT5_VENUE, "instrument_id.venue", "MT5")
            await self.load_async(instrument_id, filters)

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        PyCondition.not_none(instrument_id, "instrument_id")
        PyCondition.equal(instrument_id.venue, MT5_VENUE, "instrument_id.venue", "MT5")

        symbol = mt5_symbol_from_instrument_id(instrument_id)
        info = await self._bridge.symbol_info(symbol)
        if info is None:
            self._log.warning(f"MT5 symbol_info returned None for {symbol}: {self._bridge.last_error()}")
            return

        self._load_symbol_info(info)

    def _load_symbol_info(self, symbol_info: Any) -> None:
        try:
            instrument = parse_instrument(
                symbol_info,
                symbol_suffixes=self._config.symbol_suffixes,
                default_asset_class=self._config.default_asset_class,
            )
        except Exception as e:
            if self._log_warnings:
                self._log.warning(f"Failed parsing MT5 instrument {getattr(symbol_info, 'name', None)}: {e}")
            return

        self.add(instrument)
