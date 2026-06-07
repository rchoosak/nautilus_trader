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
from nautilus_trader.adapters.mt5.parsing import mt5_timeframe
from nautilus_trader.adapters.mt5.parsing import parse_bar
from nautilus_trader.adapters.mt5.parsing import parse_quote_tick
from nautilus_trader.adapters.mt5.parsing import to_unix_nanos
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
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BarAggregation
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
        self._health_task: asyncio.Task | None = None
        self._last_quote_signatures: dict[InstrumentId, tuple[int, str, str, str, str]] = {}
        self._last_bar_signatures: dict[str, dict[int, tuple[str, str, str, str, str]]] = {}
        self._last_health_warnings: dict[str, int] = {}

    @property
    def instrument_provider(self) -> MT5InstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._bridge.initialize()
        if not await self._ensure_bridge_connected("connect"):
            raise RuntimeError(f"MT5 terminal unavailable after connect: {self._bridge.last_error()}")
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)
        if self._config.monitor_terminal_health and self._health_task is None:
            self._health_task = self.create_task(
                self._monitor_terminal_health(),
                log_msg="mt5_data_terminal_health",
            )
        self._log.info("MT5 market data connected", LogColor.GREEN)

    async def _disconnect(self) -> None:
        for task in list(self._quote_tasks.values()) + list(self._bar_tasks.values()):
            task.cancel()
        if self._health_task is not None:
            self._health_task.cancel()
            self._health_task = None
        self._quote_tasks.clear()
        self._bar_tasks.clear()
        self._last_quote_signatures.clear()
        self._last_bar_signatures.clear()
        self._last_health_warnings.clear()
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
        if not await self._ensure_bridge_connected(f"subscribe quotes for {instrument_id}"):
            return
        await self._bridge.symbol_select(mt5_symbol_from_instrument_id(instrument_id), True)
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
        self._last_quote_signatures.pop(command.instrument_id, None)

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        if command.bar_type.spec.price_type not in (PriceType.BID, PriceType.LAST):
            self._log.warning(f"MT5 rates are bid-price bars; requested {command.bar_type.spec.price_type}")
        key = str(command.bar_type)
        if key in self._bar_tasks:
            return
        task = self.create_task(self._poll_bars(command.bar_type), log_msg=f"mt5_poll_bars_{key}")
        if task is not None:
            self._bar_tasks[key] = task

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        task = self._bar_tasks.pop(str(command.bar_type), None)
        if task is not None:
            task.cancel()
        self._last_bar_signatures.pop(str(command.bar_type), None)

    async def _poll_quotes(self, instrument_id: InstrumentId) -> None:
        symbol = mt5_symbol_from_instrument_id(instrument_id)
        interval_secs = self._config.poll_interval_ms / 1000
        while True:
            try:
                if not await self._ensure_bridge_connected(f"quote poll for {instrument_id}"):
                    self._warn_health(
                        f"quotes:{instrument_id}:disconnected",
                        f"MT5 terminal disconnected while polling quotes for {instrument_id}",
                    )
                    await asyncio.sleep(interval_secs)
                    continue

                instrument = self._instrument_provider.find(instrument_id)
                if instrument is None:
                    await self._instrument_provider.load_async(instrument_id)
                    instrument = self._instrument_provider.find(instrument_id)
                tick = await self._bridge.symbol_info_tick(symbol)
                if tick is None:
                    self._warn_health(
                        f"quotes:{instrument_id}:no_tick",
                        f"MT5 returned no tick for {instrument_id}: {self._bridge.last_error()}",
                    )
                    await asyncio.sleep(interval_secs)
                    continue
                if instrument is not None:
                    quote = parse_quote_tick(
                        instrument_id,
                        tick,
                        instrument,
                        default_size=self._config.market_depth_size,
                        ts_init=self._clock.timestamp_ns(),
                    )
                    if quote is not None and self._should_emit_quote(quote):
                        self._handle_data(quote)
                        self._warn_if_stale_quote(quote)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(f"MT5 quote poll failed for {instrument_id}: {e}")
            await asyncio.sleep(interval_secs)

    async def _poll_bars(self, bar_type: BarType) -> None:
        interval_secs = max(self._config.poll_interval_ms / 1000, 1.0)
        instrument_id = bar_type.instrument_id
        symbol = mt5_symbol_from_instrument_id(instrument_id)
        timeframe = mt5_timeframe(self._bridge.mt5, bar_type)
        while True:
            try:
                if not await self._ensure_bridge_connected(f"bar poll for {bar_type}"):
                    self._warn_health(
                        f"bars:{bar_type}:disconnected",
                        f"MT5 terminal disconnected while polling bars for {bar_type}",
                    )
                    await asyncio.sleep(interval_secs)
                    continue

                end = datetime.now(tz=UTC)
                rows = await self._bridge.copy_rates_range(
                    symbol,
                    timeframe,
                    end - self._bar_poll_lookback(bar_type),
                    end,
                )
                if rows is None:
                    self._warn_health(
                        f"bars:{bar_type}:no_rates",
                        f"MT5 returned no bars for {bar_type}: {self._bridge.last_error()}",
                    )
                    await asyncio.sleep(interval_secs)
                    continue
                instrument = self._instrument_provider.find(instrument_id)
                if rows is not None and instrument is not None:
                    for row in self._select_poll_bar_rows(rows, bar_type, end):
                        bar = parse_bar(bar_type, row, instrument, ts_init=self._clock.timestamp_ns())
                        if self._should_emit_bar(bar):
                            self._handle_data(bar)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(f"MT5 bar poll failed for {bar_type}: {e}")
            await asyncio.sleep(interval_secs)

    async def _monitor_terminal_health(self) -> None:
        interval_secs = self._config.terminal_health_check_interval_ms / 1000
        while True:
            try:
                if not await self._ensure_bridge_connected("data terminal health monitor"):
                    self._warn_health(
                        "data:terminal_health:disconnected",
                        f"MT5 terminal health check failed: {self._bridge.last_error()}",
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._warn_health("data:terminal_health:error", f"MT5 terminal health check error: {e}")
            await asyncio.sleep(interval_secs)

    async def _ensure_bridge_connected(self, reason: str) -> bool:
        connected = await self._bridge.ensure_connected(
            reconnect=self._config.reconnect_enabled,
            max_attempts=self._config.reconnect_max_attempts,
            initial_delay_ms=self._config.reconnect_initial_delay_ms,
            max_delay_ms=self._config.reconnect_max_delay_ms,
        )
        if not connected:
            self._warn_health("data:terminal:reconnect_failed", f"MT5 reconnect failed during {reason}")
        return connected

    def _should_emit_quote(self, quote: QuoteTick) -> bool:
        signature = self._quote_signature(quote)
        if self._config.deduplicate_ticks and self._last_quote_signatures.get(quote.instrument_id) == signature:
            return False
        self._last_quote_signatures[quote.instrument_id] = signature
        return True

    def _should_emit_bar(self, bar: Bar) -> bool:
        key = str(bar.bar_type)
        by_time = self._last_bar_signatures.setdefault(key, {})
        signature = self._bar_signature(bar)
        if self._config.deduplicate_bars and by_time.get(bar.ts_event) == signature:
            return False
        by_time[bar.ts_event] = signature
        if len(by_time) > 128:
            oldest = sorted(by_time)[:-128]
            for ts_event in oldest:
                by_time.pop(ts_event, None)
        return True

    def _deduplicate_quotes(self, quotes: list[QuoteTick]) -> list[QuoteTick]:
        if not self._config.deduplicate_ticks:
            return quotes
        seen: set[tuple[int, str, str, str, str]] = set()
        deduped = []
        for quote in quotes:
            signature = self._quote_signature(quote)
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(quote)
        return deduped

    def _deduplicate_bars(self, bars: list[Bar]) -> list[Bar]:
        if not self._config.deduplicate_bars:
            return bars
        seen: set[tuple[int, str, str, str, str, str]] = set()
        deduped = []
        for bar in bars:
            signature = (bar.ts_event, *self._bar_signature(bar))
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append(bar)
        return deduped

    def _quote_signature(self, quote: QuoteTick) -> tuple[int, str, str, str, str]:
        return (
            quote.ts_event,
            str(quote.bid_price),
            str(quote.ask_price),
            str(quote.bid_size),
            str(quote.ask_size),
        )

    def _bar_signature(self, bar: Bar) -> tuple[str, str, str, str, str]:
        return (
            str(bar.open),
            str(bar.high),
            str(bar.low),
            str(bar.close),
            str(bar.volume),
        )

    def _select_poll_bar_rows(self, rows: Any, bar_type: BarType, now: datetime) -> list[Any]:
        closed_rows = []
        forming_rows = []
        for row in rows:
            if self._is_closed_bar_row(row, bar_type, now):
                closed_rows.append(row)
            else:
                forming_rows.append(row)

        selected = []
        if self._config.emit_closed_bars and closed_rows:
            selected.append(closed_rows[-1])
        if self._config.emit_forming_bars and forming_rows:
            selected.append(forming_rows[-1])
        return sorted(selected, key=lambda row: to_unix_nanos(self._bar_row_time(row)))

    def _is_closed_bar_row(self, row: Any, bar_type: BarType, now: datetime) -> bool:
        duration = self._bar_duration(bar_type)
        if duration is None:
            return True
        bar_start_ns = to_unix_nanos(self._bar_row_time(row))
        bar_end_ns = bar_start_ns + int(duration.total_seconds() * 1_000_000_000)
        return bar_end_ns <= int(now.timestamp() * 1_000_000_000)

    def _bar_poll_lookback(self, bar_type: BarType) -> timedelta:
        duration = self._bar_duration(bar_type) or timedelta(days=31 * bar_type.spec.step)
        lookback = duration * self._config.bar_poll_lookback_multiplier
        return max(lookback, timedelta(minutes=5))

    def _bar_duration(self, bar_type: BarType) -> timedelta | None:
        step = bar_type.spec.step
        aggregation = bar_type.spec.aggregation
        if aggregation == BarAggregation.MINUTE:
            return timedelta(minutes=step)
        if aggregation == BarAggregation.HOUR:
            return timedelta(hours=step)
        if aggregation == BarAggregation.DAY:
            return timedelta(days=step)
        if aggregation == BarAggregation.WEEK:
            return timedelta(weeks=step)
        return None

    def _bar_row_time(self, row: Any) -> Any:
        if isinstance(row, dict):
            return row.get("time")
        if hasattr(row, "time"):
            return row.time
        return row["time"]

    def _warn_if_stale_quote(self, quote: QuoteTick) -> None:
        if not self._config.warn_on_stale_ticks or quote.ts_event <= 0:
            return
        now = self._clock.timestamp_ns()
        age_ns = now - quote.ts_event
        threshold_ns = self._config.stale_tick_threshold_ms * 1_000_000
        if age_ns > threshold_ns:
            self._warn_health(
                f"quotes:{quote.instrument_id}:stale",
                f"MT5 stale tick for {quote.instrument_id}: age_ms={age_ns // 1_000_000}",
            )

    def _warn_health(self, key: str, message: str) -> None:
        now = self._clock.timestamp_ns()
        interval_ns = self._config.health_warning_interval_ms * 1_000_000
        last = self._last_health_warnings.get(key, 0)
        if now - last < interval_ns:
            return
        self._last_health_warnings[key] = now
        self._log.warning(message)

    async def _request_instrument(self, request: RequestInstrument) -> None:
        await self._instrument_provider.load_async(request.instrument_id)
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is not None:
            self._handle_instrument(instrument, request.id, request.start, request.end, request.params)

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
        if not await self._ensure_bridge_connected(f"request quotes for {request.instrument_id}"):
            return
        start = request.start or (datetime.now(tz=UTC) - timedelta(hours=1))
        end = request.end or datetime.now(tz=UTC)
        rows = await self._bridge.copy_ticks_range(symbol, start, end, self._bridge.mt5.COPY_TICKS_INFO)
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
        quotes = self._deduplicate_quotes(quotes)
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
            self._log.error(f"Cannot request internally aggregated MT5 bars: {request.bar_type}")
            return
        instrument_id = request.bar_type.instrument_id
        symbol = mt5_symbol_from_instrument_id(instrument_id)
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(instrument_id)
            instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot request bars for unknown instrument {instrument_id}")
            return
        if not await self._ensure_bridge_connected(f"request bars for {request.bar_type}"):
            return
        start = request.start or (datetime.now(tz=UTC) - timedelta(days=1))
        end = request.end or datetime.now(tz=UTC)
        timeframe = mt5_timeframe(self._bridge.mt5, request.bar_type)
        rows = await self._bridge.copy_rates_range(symbol, timeframe, start, end)
        bars = []
        if rows is not None:
            for row in rows:
                bars.append(parse_bar(request.bar_type, row, instrument, ts_init=self._clock.timestamp_ns()))
        bars = self._deduplicate_bars(bars)
        if request.limit > 0:
            bars = bars[-request.limit :]
        self._handle_bars(request.bar_type, bars, request.id, request.start, request.end, request.params)
