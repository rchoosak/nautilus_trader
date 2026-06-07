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
Live market data client for MetaTrader 5.
"""

from __future__ import annotations

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge
from nautilus_trader.adapters.mt5.config import MT5DataClientConfig
from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.adapters.mt5.parsing import get_field
from nautilus_trader.adapters.mt5.parsing import mt5_timeframe
from nautilus_trader.adapters.mt5.parsing import parse_bar
from nautilus_trader.adapters.mt5.parsing import parse_quote_tick
from nautilus_trader.adapters.mt5.providers import MT5InstrumentProvider
from nautilus_trader.adapters.mt5.symbol import mt5_symbol_from_instrument_id
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import RequestQuoteTicks
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeInstrument
from nautilus_trader.data.messages import SubscribeInstruments
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeInstrument
from nautilus_trader.data.messages import UnsubscribeInstruments
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId


class MT5DataClient(LiveMarketDataClient):
    """
    Provides live and historical market data from a MetaTrader 5 terminal.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        bridge: MT5TerminalBridge,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: MT5InstrumentProvider,
        config: MT5DataClientConfig,
        name: str | None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or MT5_VENUE.value),
            venue=config.venue,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )

        self._bridge = bridge
        self._config = config
        self._quote_tasks: dict[InstrumentId, asyncio.Task] = {}
        self._bar_tasks: dict[str, asyncio.Task] = {}

        self._log.info(f"{config.path=}", LogColor.BLUE)
        self._log.info(f"{config.server=}", LogColor.BLUE)
        self._log.info(f"login set: {config.login is not None}", LogColor.BLUE)
        self._log.info(f"{config.poll_interval_ms=}", LogColor.BLUE)

    @property
    def instrument_provider(self) -> MT5InstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._bridge.initialize()
        await self._instrument_provider.initialize()

        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
            try:
                self._cache.add_currency(instrument.quote_currency)
                if hasattr(instrument, "base_currency") and instrument.base_currency:
                    self._cache.add_currency(instrument.base_currency)
            except Exception as e:
                self._log.debug(f"Failed caching MT5 instrument currencies: {e}")

        self._log.info("MT5 market data connected", LogColor.GREEN)

    async def _disconnect(self) -> None:
        for task in list(self._quote_tasks.values()) + list(self._bar_tasks.values()):
            task.cancel()
        self._quote_tasks.clear()
        self._bar_tasks.clear()
        await self._bridge.shutdown()

    async def _subscribe_instruments(self, command: SubscribeInstruments) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)

    async def _unsubscribe_instruments(self, command: UnsubscribeInstruments) -> None:
        return None

    async def _subscribe_instrument(self, command: SubscribeInstrument) -> None:
        instrument = self._instrument_provider.find(command.instrument_id)
        if instrument is not None:
            self._handle_data(instrument)

    async def _unsubscribe_instrument(self, command: UnsubscribeInstrument) -> None:
        return None

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._quote_tasks:
            return

        symbol = mt5_symbol_from_instrument_id(instrument_id)
        await self._bridge.symbol_select(symbol, True)

        task = self.create_task(
            self._poll_quotes(instrument_id),
            log_msg=f"mt5_poll_quotes_{instrument_id}",
        )
        if task is not None:
            self._quote_tasks[instrument_id] = task

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        task = self._quote_tasks.pop(command.instrument_id, None)
        if task is not None:
            task.cancel()

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        if command.bar_type.spec.price_type not in (PriceType.BID, PriceType.LAST):
            self._log.warning(
                f"MT5 external bars are bid-price bars; requested {command.bar_type.spec.price_type}",
            )

        key = str(command.bar_type)
        if key in self._bar_tasks:
            return

        task = self.create_task(
            self._poll_bars(command.bar_type),
            log_msg=f"mt5_poll_bars_{key}",
        )
        if task is not None:
            self._bar_tasks[key] = task

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        task = self._bar_tasks.pop(str(command.bar_type), None)
        if task is not None:
            task.cancel()

    async def _poll_quotes(self, instrument_id: InstrumentId) -> None:
        symbol = mt5_symbol_from_instrument_id(instrument_id)
        interval_secs = self._config.poll_interval_ms / 1000

        while True:
            try:
                instrument = self._instrument_provider.find(instrument_id)
                if instrument is None:
                    await self._instrument_provider.load_async(instrument_id)
                    instrument = self._instrument_provider.find(instrument_id)
                if instrument is None:
                    self._log.warning(f"Cannot poll {instrument_id}: instrument not loaded")
                    await asyncio.sleep(interval_secs)
                    continue

                tick = await self._bridge.symbol_info_tick(symbol)
                quote = parse_quote_tick(
                    instrument_id,
                    tick,
                    instrument,
                    default_size=self._config.market_depth_size,
                    ts_init=self._clock.timestamp_ns(),
                )
                if quote is not None:
                    self._handle_data(quote)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(f"MT5 quote poll failed for {instrument_id}: {e}")

            await asyncio.sleep(interval_secs)

    async def _poll_bars(self, bar_type: Any) -> None:
        # MT5 terminal bars are polled by repeatedly requesting the most recent range.
        # The final row returned is the still-forming bar, so only the closed bars (all
        # but the last) are emitted, and a watermark on the bar open time prevents the
        # same closed bar from being emitted more than once.
        interval_secs = max(self._config.poll_interval_ms / 1000, 1.0)
        instrument_id = bar_type.instrument_id
        symbol = mt5_symbol_from_instrument_id(instrument_id)
        timeframe = mt5_timeframe(self._bridge.mt5, bar_type)

        # Request a window wide enough to always include at least one closed bar.
        # ``timedelta`` is undefined for month bars, so fall back to a wide window.
        try:
            lookback = bar_type.spec.timedelta * 5
        except ValueError:
            lookback = timedelta(days=370)
        last_open: int | None = None

        while True:
            try:
                end = datetime.now(tz=UTC)
                start = end - lookback
                rows = await self._bridge.copy_rates_range(symbol, timeframe, start, end)
                instrument = self._instrument_provider.find(instrument_id)
                if rows is not None and instrument is not None and len(rows) > 1:
                    closed_rows = rows[:-1]
                    if last_open is None:
                        # Prime the watermark so only bars closing after subscription
                        # are emitted; historical bars come from a bars request.
                        last_open = get_field(closed_rows[-1], "time")
                    else:
                        for row in closed_rows:
                            open_time = get_field(row, "time")
                            if open_time <= last_open:
                                continue
                            self._handle_data(
                                parse_bar(
                                    bar_type,
                                    row,
                                    instrument,
                                    ts_init=self._clock.timestamp_ns(),
                                ),
                            )
                            last_open = open_time
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(f"MT5 bar poll failed for {bar_type}: {e}")

            await asyncio.sleep(interval_secs)

    async def _request_instrument(self, request: RequestInstrument) -> None:
        await self._instrument_provider.load_async(request.instrument_id)
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is not None:
            self._handle_instrument(
                instrument,
                request.id,
                request.start,
                request.end,
                request.params,
            )

    async def _request_instruments(self, request: RequestInstruments) -> None:
        await self._instrument_provider.load_all_async(request.params)
        self._handle_instruments(
            self.venue,
            list(self._instrument_provider.get_all().values()),
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        symbol = mt5_symbol_from_instrument_id(request.instrument_id)
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(request.instrument_id)
            instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request quotes for unknown instrument {request.instrument_id}")
            return

        start = request.start or (datetime.now(tz=UTC) - timedelta(hours=1))
        end = request.end or datetime.now(tz=UTC)
        flags = self._bridge.mt5.COPY_TICKS_INFO
        rows = await self._bridge.copy_ticks_range(symbol, start, end, flags)

        quotes = []
        if rows is not None:
            for row in rows:
                quote = parse_quote_tick(
                    request.instrument_id,
                    row,
                    instrument,
                    default_size=self._config.market_depth_size,
                    ts_init=self._clock.timestamp_ns(),
                )
                if quote is not None:
                    quotes.append(quote)

        if request.limit > 0:
            quotes = quotes[-request.limit :]

        self._handle_quote_ticks(
            request.instrument_id,
            quotes,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        if request.bar_type.is_internally_aggregated():
            self._log.error(
                f"Cannot request {request.bar_type}: MT5 only provides external bars. "
                "Subscribe to QuoteTick data for internal aggregation.",
            )
            return

        if request.bar_type.spec.price_type not in (PriceType.BID, PriceType.LAST):
            self._log.warning(
                f"MT5 rates are bid-price bars; requested {request.bar_type.spec.price_type}",
            )

        instrument_id = request.bar_type.instrument_id
        symbol = mt5_symbol_from_instrument_id(instrument_id)
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(instrument_id)
            instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request bars for unknown instrument {instrument_id}")
            return

        start = request.start or (datetime.now(tz=UTC) - timedelta(days=1))
        end = request.end or datetime.now(tz=UTC)
        timeframe = mt5_timeframe(self._bridge.mt5, request.bar_type)
        rows = await self._bridge.copy_rates_range(symbol, timeframe, start, end)

        bars = []
        if rows is not None:
            for row in rows:
                bars.append(parse_bar(request.bar_type, row, instrument, ts_init=self._clock.timestamp_ns()))

        if request.limit > 0:
            bars = bars[-request.limit :]

        self._handle_bars(
            request.bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )
