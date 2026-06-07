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
Live execution client for MetaTrader 5.
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge
from nautilus_trader.adapters.mt5.config import MT5ExecClientConfig
from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.adapters.mt5.parsing import get_field
from nautilus_trader.adapters.mt5.parsing import market_filling_mode
from nautilus_trader.adapters.mt5.parsing import mt5_time_to_nanos
from nautilus_trader.adapters.mt5.parsing import order_side_from_mt5
from nautilus_trader.adapters.mt5.parsing import order_status_from_mt5
from nautilus_trader.adapters.mt5.parsing import order_type_from_mt5
from nautilus_trader.adapters.mt5.parsing import parse_client_order_id
from nautilus_trader.adapters.mt5.parsing import zero_money
from nautilus_trader.adapters.mt5.providers import MT5InstrumentProvider
from nautilus_trader.adapters.mt5.symbol import instrument_id_from_mt5_symbol
from nautilus_trader.adapters.mt5.symbol import mt5_symbol_from_instrument_id
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryAccount
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money


class MT5ExecutionClient(LiveExecutionClient):
    """
    Provides order execution through a MetaTrader 5 terminal.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        bridge: MT5TerminalBridge,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: MT5InstrumentProvider,
        config: MT5ExecClientConfig,
        name: str | None,
    ) -> None:
        client_id = ClientId(name or MT5_VENUE.value)
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=config.venue,
            oms_type=config.oms_type,
            account_type=AccountType.MARGIN,
            base_currency=None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )

        self._bridge = bridge
        self._config = config
        self._client_id = client_id
        self._client_order_ids_by_comment: dict[str, ClientOrderId] = {}

        self._log.info(f"{config.path=}", LogColor.BLUE)
        self._log.info(f"{config.server=}", LogColor.BLUE)
        self._log.info(f"login set: {config.login is not None}", LogColor.BLUE)
        self._log.info(f"{config.magic=}", LogColor.BLUE)
        self._log.info(f"{config.oms_type=}", LogColor.BLUE)

    @property
    def instrument_provider(self) -> MT5InstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._bridge.initialize()
        await self._instrument_provider.initialize()
        await self._update_account_state()
        await self._await_account_registered()
        self._log.info("MT5 execution connected", LogColor.GREEN)

    async def _disconnect(self) -> None:
        await self._bridge.shutdown()

    def _account_id_from_info(self, info: Any | None) -> AccountId:
        raw = self._config.account_id or str(get_field(info, "login", self._config.login or "UNKNOWN"))
        if raw.startswith(f"{self._client_id.value}-"):
            return AccountId(raw)
        return AccountId(f"{self._client_id.value}-{raw}")

    async def _update_account_state(self) -> None:
        info = await self._bridge.account_info()
        if info is None:
            raise RuntimeError(f"MT5 account_info returned None: {self._bridge.last_error()}")

        account_id = self._account_id_from_info(info)
        if self.account_id is None or self.account_id != account_id:
            self._set_account_id(account_id)

        # The account is a multi-currency margin account (base_currency=None), so the
        # MT5 account currency is only used to denominate the reported balances.
        currency = Currency.from_str(get_field(info, "currency", "USD"), strict=False)

        locked = Decimal(str(get_field(info, "margin", 0) or 0))
        free = Decimal(str(get_field(info, "margin_free", 0) or 0))
        total = locked + free
        if total == 0:
            total = Decimal(str(get_field(info, "equity", 0) or get_field(info, "balance", 0) or 0))
            free = total

        balance = AccountBalance(
            total=Money(total, currency),
            locked=Money(locked, currency),
            free=Money(free, currency),
        )

        self.generate_account_state(
            balances=[balance],
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info=dict(info._asdict()) if hasattr(info, "_asdict") else {},
        )

    async def _query_account(self, command: QueryAccount) -> None:
        await self._update_account_state()

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order
        instrument = self._cache.instrument(command.instrument_id) or self._instrument_provider.find(
            command.instrument_id,
        )
        if instrument is None:
            await self._instrument_provider.load_async(command.instrument_id)
            instrument = self._instrument_provider.find(command.instrument_id)
        if instrument is None:
            self.generate_order_denied(
                command.strategy_id,
                command.instrument_id,
                order.client_order_id,
                f"INSTRUMENT_NOT_FOUND: {command.instrument_id}",
                self._clock.timestamp_ns(),
            )
            return

        try:
            request = await self._order_to_mt5_request(command, instrument)
        except Exception as e:
            self.generate_order_denied(
                command.strategy_id,
                command.instrument_id,
                order.client_order_id,
                str(e),
                self._clock.timestamp_ns(),
            )
            return

        self.generate_order_submitted(
            command.strategy_id,
            command.instrument_id,
            order.client_order_id,
            self._clock.timestamp_ns(),
        )

        try:
            result = await self._bridge.order_send(request)
        except Exception as e:
            self._log.error(
                f"MT5 submit outcome unknown for {order.client_order_id}: {e}. "
                "Leaving order in-flight for reconciliation.",
            )
            return

        await self._handle_submit_result(command, result, instrument)

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        for order in command.order_list.orders:
            await self._submit_order(
                SubmitOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    order=order,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                    position_id=command.position_id,
                    client_id=command.client_id,
                    params=command.params,
                ),
            )

    async def _modify_order(self, command: ModifyOrder) -> None:
        venue_order_id = command.venue_order_id or self._cache.venue_order_id(command.client_order_id)
        if venue_order_id is None:
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                "VENUE_ORDER_ID_NOT_FOUND",
                self._clock.timestamp_ns(),
            )
            return

        cached_order = self._cache.order(command.client_order_id)
        quantity = command.quantity or (cached_order.quantity if cached_order is not None else None)
        price = command.price or (
            cached_order.price if cached_order is not None and cached_order.has_price else None
        )
        trigger_price = command.trigger_price or (
            cached_order.trigger_price
            if cached_order is not None and cached_order.has_trigger_price
            else None
        )

        if quantity is None:
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                "ORDER_QUANTITY_NOT_FOUND",
                self._clock.timestamp_ns(),
            )
            return

        request: dict[str, Any] = {
            "action": self._bridge.mt5.TRADE_ACTION_MODIFY,
            "order": int(venue_order_id.value),
            "magic": self._config.magic,
            "comment": self._order_comment(command.client_order_id),
        }
        self._apply_pending_price_fields(request, price=price, trigger_price=trigger_price)

        result = await self._bridge.order_send(request)
        retcode = get_field(result, "retcode")
        if retcode in self._success_retcodes():
            self.generate_order_updated(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                quantity,
                price,
                trigger_price,
                self._clock.timestamp_ns(),
            )
        else:
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                self._retcode_reason(result),
                self._clock.timestamp_ns(),
            )

    async def _cancel_order(self, command: CancelOrder) -> None:
        venue_order_id = command.venue_order_id or self._cache.venue_order_id(command.client_order_id)
        if venue_order_id is None:
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                "VENUE_ORDER_ID_NOT_FOUND",
                self._clock.timestamp_ns(),
            )
            return

        result = await self._bridge.order_send(
            {
                "action": self._bridge.mt5.TRADE_ACTION_REMOVE,
                "order": int(venue_order_id.value),
                "magic": self._config.magic,
                "comment": self._order_comment(command.client_order_id),
            },
        )
        retcode = get_field(result, "retcode")
        if retcode in self._success_retcodes():
            self.generate_order_canceled(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                self._clock.timestamp_ns(),
            )
        else:
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                self._retcode_reason(result),
                self._clock.timestamp_ns(),
            )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        kwargs = {}
        if command.instrument_id is not None:
            kwargs["symbol"] = mt5_symbol_from_instrument_id(command.instrument_id)

        orders = await self._bridge.orders_get(**kwargs) or []
        for mt5_order in orders:
            if get_field(mt5_order, "magic") != self._config.magic:
                continue

            client_order_id = self._client_order_id_from_comment(get_field(mt5_order, "comment"))
            if client_order_id is None:
                continue

            instrument_id = instrument_id_from_mt5_symbol(get_field(mt5_order, "symbol"))
            await self._cancel_order(
                CancelOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=VenueOrderId(str(get_field(mt5_order, "ticket"))),
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                ),
            )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    @staticmethod
    def _apply_pending_price_fields(
        request: dict[str, Any],
        *,
        price: Any | None,
        trigger_price: Any | None,
    ) -> None:
        # Maps Nautilus limit/trigger prices onto the MT5 request fields, shared by order
        # submission and modification so the two paths cannot diverge. MT5 carries the
        # trigger in ``price`` and the stop-limit's limit price in ``stoplimit``:
        #   LIMIT       -> price=limit
        #   STOP_MARKET -> price=trigger
        #   STOP_LIMIT  -> price=trigger, stoplimit=limit
        if trigger_price is not None:
            request["price"] = float(trigger_price)
            if price is not None:
                request["stoplimit"] = float(price)
        elif price is not None:
            request["price"] = float(price)

    async def _order_to_mt5_request(self, command: SubmitOrder, instrument: Any) -> dict[str, Any]:
        mt5 = self._bridge.mt5
        order = command.order
        symbol = mt5_symbol_from_instrument_id(command.instrument_id)
        await self._bridge.symbol_select(symbol, True)

        volume = float(order.quantity.as_decimal())
        comment = self._order_comment(order.client_order_id)
        request: dict[str, Any] = {
            "symbol": symbol,
            "volume": volume,
            "magic": self._config.magic,
            "deviation": self._config.deviation,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
        }

        if order.order_type == OrderType.MARKET:
            tick = await self._bridge.symbol_info_tick(symbol)
            if tick is None:
                raise RuntimeError(f"NO_TICK_FOR_SYMBOL: {symbol}")
            is_buy = order.side == OrderSide.BUY
            request["action"] = mt5.TRADE_ACTION_DEAL
            request["type"] = getattr(mt5, "ORDER_TYPE_BUY" if is_buy else "ORDER_TYPE_SELL")
            request["price"] = float(get_field(tick, "ask" if is_buy else "bid"))
            request["type_filling"] = market_filling_mode(mt5, instrument)
        elif order.order_type == OrderType.LIMIT:
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["type"] = getattr(
                mt5,
                "ORDER_TYPE_BUY_LIMIT" if order.side == OrderSide.BUY else "ORDER_TYPE_SELL_LIMIT",
            )
            request["type_filling"] = mt5.ORDER_FILLING_RETURN
            self._apply_pending_price_fields(request, price=order.price, trigger_price=None)
        elif order.order_type == OrderType.STOP_MARKET:
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["type"] = getattr(
                mt5,
                "ORDER_TYPE_BUY_STOP" if order.side == OrderSide.BUY else "ORDER_TYPE_SELL_STOP",
            )
            request["type_filling"] = mt5.ORDER_FILLING_RETURN
            self._apply_pending_price_fields(request, price=None, trigger_price=order.trigger_price)
        elif order.order_type == OrderType.STOP_LIMIT:
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["type"] = getattr(
                mt5,
                "ORDER_TYPE_BUY_STOP_LIMIT" if order.side == OrderSide.BUY else "ORDER_TYPE_SELL_STOP_LIMIT",
            )
            request["type_filling"] = mt5.ORDER_FILLING_RETURN
            self._apply_pending_price_fields(
                request,
                price=order.price,
                trigger_price=order.trigger_price,
            )
        else:
            raise ValueError(f"Unsupported MT5 order type: {order.order_type}")

        return request

    async def _handle_submit_result(self, command: SubmitOrder, result: Any, instrument: Any) -> None:
        order = command.order
        if result is None:
            self._log.error(
                f"MT5 submit returned None for {order.client_order_id}: {self._bridge.last_error()}. "
                "Leaving order in-flight for reconciliation.",
            )
            return

        retcode = get_field(result, "retcode")
        if retcode not in self._success_retcodes():
            self.generate_order_rejected(
                command.strategy_id,
                command.instrument_id,
                order.client_order_id,
                self._retcode_reason(result),
                self._clock.timestamp_ns(),
            )
            return

        venue_order_id = VenueOrderId(str(get_field(result, "order") or get_field(result, "deal")))
        self._cache.add_venue_order_id(order.client_order_id, venue_order_id)
        self.generate_order_accepted(
            command.strategy_id,
            command.instrument_id,
            order.client_order_id,
            venue_order_id,
            self._clock.timestamp_ns(),
        )

        deal = get_field(result, "deal", 0) or 0
        if deal:
            last_px = instrument.make_price(get_field(result, "price"))
            last_qty = instrument.make_qty(get_field(result, "volume") or order.quantity.as_decimal())
            self.generate_order_filled(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=TradeId(str(deal)),
                order_side=order.side,
                order_type=order.order_type,
                last_qty=last_qty,
                last_px=last_px,
                quote_currency=instrument.quote_currency,
                commission=zero_money(instrument.quote_currency),
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event=self._clock.timestamp_ns(),
                info=dict(result._asdict()) if hasattr(result, "_asdict") else {},
            )

    def _success_retcodes(self) -> set[int]:
        mt5 = self._bridge.mt5
        return {
            mt5.TRADE_RETCODE_DONE,
            mt5.TRADE_RETCODE_PLACED,
            mt5.TRADE_RETCODE_DONE_PARTIAL,
        }

    def _retcode_reason(self, result: Any) -> str:
        retcode = get_field(result, "retcode", "UNKNOWN")
        comment = get_field(result, "comment", "")
        return f"MT5_RETCODE_{retcode}: {comment}"

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        reports = await self.generate_order_status_reports(
            GenerateOrderStatusReports(
                instrument_id=command.instrument_id,
                start=None,
                end=None,
                open_only=False,
                command_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            ),
        )
        for report in reports:
            if command.venue_order_id is not None and report.venue_order_id == command.venue_order_id:
                return report
            if command.client_order_id is not None and report.client_order_id == command.client_order_id:
                return report
        return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        reports: list[OrderStatusReport] = []
        symbol = mt5_symbol_from_instrument_id(command.instrument_id) if command.instrument_id else None

        active_orders = await self._bridge.orders_get(**({"symbol": symbol} if symbol else {})) or []
        for mt5_order in active_orders:
            if not self._is_own_record(mt5_order):
                continue
            try:
                reports.append(await self._parse_order_status_report(mt5_order, active=True))
            except Exception as e:
                self._log.warning(f"Failed parsing MT5 active order report: {e}")

        if not command.open_only:
            start = command.start or (datetime.now(tz=UTC) - timedelta(days=30))
            end = command.end or datetime.now(tz=UTC)
            history = await self._bridge.history_orders_get(start, end, **({"symbol": symbol} if symbol else {})) or []
            for mt5_order in history:
                if not self._is_own_record(mt5_order):
                    continue
                try:
                    reports.append(await self._parse_order_status_report(mt5_order, active=False))
                except Exception as e:
                    self._log.warning(f"Failed parsing MT5 history order report: {e}")

        return reports

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        symbol = mt5_symbol_from_instrument_id(command.instrument_id) if command.instrument_id else None
        start = command.start or (datetime.now(tz=UTC) - timedelta(days=30))
        end = command.end or datetime.now(tz=UTC)
        deals = await self._bridge.history_deals_get(start, end, **({"symbol": symbol} if symbol else {})) or []

        reports = []
        for deal in deals:
            if not self._is_own_record(deal):
                continue
            if command.venue_order_id and str(get_field(deal, "order")) != command.venue_order_id.value:
                continue
            try:
                report = await self._parse_fill_report(deal)
            except Exception as e:
                self._log.warning(f"Failed parsing MT5 fill report: {e}")
                continue
            if report is not None:
                reports.append(report)
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        symbol = mt5_symbol_from_instrument_id(command.instrument_id) if command.instrument_id else None
        positions = await self._bridge.positions_get(**({"symbol": symbol} if symbol else {})) or []

        reports = []
        for position in positions:
            if not self._is_own_record(position):
                continue
            try:
                report = await self._parse_position_status_report(position)
            except Exception as e:
                self._log.warning(f"Failed parsing MT5 position report: {e}")
                continue
            if report is not None:
                reports.append(report)
        return reports

    async def _parse_order_status_report(self, mt5_order: Any, *, active: bool) -> OrderStatusReport:
        mt5 = self._bridge.mt5
        symbol = get_field(mt5_order, "symbol")
        instrument_id = instrument_id_from_mt5_symbol(symbol)
        instrument = await self._ensure_instrument(instrument_id)
        raw_quantity = get_field(mt5_order, "volume_initial") or get_field(mt5_order, "volume_current") or 0
        if Decimal(str(raw_quantity)) <= 0:
            raise ValueError(f"MT5 order {get_field(mt5_order, 'ticket')} has non-positive quantity")
        quantity = instrument.make_qty(raw_quantity)
        remaining = instrument.make_qty(get_field(mt5_order, "volume_current") or 0)
        filled = quantity.saturating_sub(remaining)
        mt5_type = get_field(mt5_order, "type")
        price = get_field(mt5_order, "price_open", 0) or 0
        position_id = get_field(mt5_order, "position_id", 0) or 0

        return OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            client_order_id=self._client_order_id_from_comment(get_field(mt5_order, "comment")),
            venue_order_id=VenueOrderId(str(get_field(mt5_order, "ticket"))),
            venue_position_id=PositionId(str(position_id)) if position_id else None,
            order_side=order_side_from_mt5(mt5, mt5_type),
            order_type=order_type_from_mt5(mt5, mt5_type),
            time_in_force=TimeInForce.GTC,
            order_status=order_status_from_mt5(mt5, get_field(mt5_order, "state"), active=active),
            quantity=quantity,
            filled_qty=filled,
            price=instrument.make_price(price) if price else None,
            avg_px=Decimal(str(get_field(mt5_order, "price_current", 0) or price or 0)),
            report_id=UUID4(),
            ts_accepted=mt5_time_to_nanos(mt5_order, "time_setup"),
            ts_last=mt5_time_to_nanos(mt5_order, "time_done", "time_setup"),
            ts_init=self._clock.timestamp_ns(),
        )

    async def _parse_fill_report(self, deal: Any) -> FillReport | None:
        volume = get_field(deal, "volume", 0) or 0
        if volume <= 0:
            return None

        symbol = get_field(deal, "symbol")
        instrument_id = instrument_id_from_mt5_symbol(symbol)
        instrument = await self._ensure_instrument(instrument_id)
        mt5_type = get_field(deal, "type")
        side = OrderSide.SELL if mt5_type == getattr(self._bridge.mt5, "DEAL_TYPE_SELL", 1) else OrderSide.BUY
        commission = Decimal(str(get_field(deal, "commission", 0) or 0)).copy_abs()
        position_id = get_field(deal, "position_id", 0) or 0

        return FillReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            client_order_id=self._client_order_id_from_comment(get_field(deal, "comment")),
            venue_order_id=VenueOrderId(str(get_field(deal, "order"))),
            venue_position_id=PositionId(str(position_id)) if position_id else None,
            trade_id=TradeId(str(get_field(deal, "ticket"))),
            order_side=side,
            last_qty=instrument.make_qty(volume),
            last_px=instrument.make_price(get_field(deal, "price")),
            commission=Money(commission, instrument.quote_currency),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            report_id=UUID4(),
            ts_event=mt5_time_to_nanos(deal, "time"),
            ts_init=self._clock.timestamp_ns(),
        )

    async def _parse_position_status_report(self, position: Any) -> PositionStatusReport | None:
        volume = get_field(position, "volume", 0) or 0
        if volume <= 0:
            return None

        instrument_id = instrument_id_from_mt5_symbol(get_field(position, "symbol"))
        instrument = await self._ensure_instrument(instrument_id)
        position_id = get_field(position, "ticket", 0) or get_field(position, "identifier", 0) or 0
        side = (
            PositionSide.SHORT
            if get_field(position, "type") == getattr(self._bridge.mt5, "POSITION_TYPE_SELL", 1)
            else PositionSide.LONG
        )

        return PositionStatusReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            venue_position_id=PositionId(str(position_id)) if position_id else None,
            position_side=side,
            quantity=instrument.make_qty(volume),
            avg_px_open=Decimal(str(get_field(position, "price_open", 0) or 0)),
            report_id=UUID4(),
            ts_last=mt5_time_to_nanos(position, "time_update", "time"),
            ts_init=self._clock.timestamp_ns(),
        )

    async def _ensure_instrument(self, instrument_id: InstrumentId) -> Any:
        instrument = self._cache.instrument(instrument_id) or self._instrument_provider.find(instrument_id)
        if instrument is None:
            await self._instrument_provider.load_async(instrument_id)
            instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            raise RuntimeError(f"MT5 instrument not loaded: {instrument_id}")
        return instrument

    def _is_own_record(self, record: Any) -> bool:
        magic = get_field(record, "magic", None)
        return magic == self._config.magic

    def _order_comment(self, client_order_id: ClientOrderId) -> str:
        # MT5 order comments are capped at 31 chars. Client order IDs within that limit
        # are stored verbatim and recovered on reconciliation; longer IDs are hashed to
        # ``NT:<digest>`` and can only be mapped back via the in-memory table (lost on
        # restart). Keep client order IDs <= 31 chars to preserve reconciliation linkage.
        value = client_order_id.value
        if len(value) <= 31:
            comment = value
        else:
            digest = hashlib.blake2s(value.encode("utf-8"), digest_size=14).hexdigest()
            comment = f"NT:{digest}"
        self._client_order_ids_by_comment[comment] = client_order_id
        return comment

    def _client_order_id_from_comment(self, comment: str | None) -> ClientOrderId | None:
        if not comment:
            return None
        mapped = self._client_order_ids_by_comment.get(comment)
        if mapped is not None:
            return mapped
        if comment.startswith("NT:"):
            return None
        return parse_client_order_id(comment)
