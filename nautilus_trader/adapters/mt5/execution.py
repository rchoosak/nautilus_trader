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
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge
from nautilus_trader.adapters.mt5.config import MT5ExecClientConfig
from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.adapters.mt5.parsing import MT5SymbolRules
from nautilus_trader.adapters.mt5.parsing import get_field
from nautilus_trader.adapters.mt5.parsing import order_side_from_mt5
from nautilus_trader.adapters.mt5.parsing import order_status_from_mt5
from nautilus_trader.adapters.mt5.parsing import order_type_from_mt5
from nautilus_trader.adapters.mt5.parsing import parse_client_order_id
from nautilus_trader.adapters.mt5.parsing import parse_symbol_rules
from nautilus_trader.adapters.mt5.parsing import to_unix_nanos
from nautilus_trader.adapters.mt5.parsing import utc_from_ns
from nautilus_trader.adapters.mt5.parsing import zero_money
from nautilus_trader.adapters.mt5.providers import MT5InstrumentProvider
from nautilus_trader.adapters.mt5.reconciliation import client_order_id_from_comment
from nautilus_trader.adapters.mt5.reconciliation import client_order_id_from_deal_ticket
from nautilus_trader.adapters.mt5.reconciliation import client_order_id_from_venue_order_id
from nautilus_trader.adapters.mt5.reconciliation import comment_mappings
from nautilus_trader.adapters.mt5.reconciliation import decode_reconciliation_payload
from nautilus_trader.adapters.mt5.reconciliation import empty_reconciliation_payload
from nautilus_trader.adapters.mt5.reconciliation import encode_reconciliation_payload
from nautilus_trader.adapters.mt5.reconciliation import register_order_mapping
from nautilus_trader.adapters.mt5.reconciliation import venue_order_mappings
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
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import oms_type_to_str
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import MarginBalance
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
        self._reconciliation_mappings: dict[str, Any] = empty_reconciliation_payload()
        self._reconciliation_mappings_loaded = False
        self._health_task: asyncio.Task | None = None

    @property
    def instrument_provider(self) -> MT5InstrumentProvider:
        return self._instrument_provider

    async def _connect(self) -> None:
        await self._bridge.initialize()
        if not await self._ensure_bridge_connected("connect"):
            raise RuntimeError(f"MT5 terminal unavailable after connect: {self._bridge.last_error()}")
        await self._instrument_provider.initialize()
        await self._update_account_state()
        self._load_reconciliation_mappings()
        await self._await_account_registered()
        if self._config.monitor_terminal_health and self._health_task is None:
            self._health_task = self.create_task(
                self._monitor_terminal_health(),
                log_msg="mt5_exec_terminal_health",
            )
        self._log.info("MT5 execution connected", LogColor.GREEN)

    async def _disconnect(self) -> None:
        if self._health_task is not None:
            self._health_task.cancel()
            self._health_task = None
        await self._bridge.shutdown()

    def _account_id_from_info(self, info: Any | None) -> AccountId:
        raw = self._config.account_id or str(get_field(info, "login", self._config.login or "UNKNOWN"))
        if raw.startswith(f"{self._client_id.value}-"):
            return AccountId(raw)
        return AccountId(f"{self._client_id.value}-{raw}")

    async def _update_account_state(self) -> None:
        if not await self._ensure_bridge_connected("account state update"):
            raise RuntimeError(f"MT5 terminal unavailable for account state: {self._bridge.last_error()}")
        info = await self._bridge.account_info()
        if info is None:
            raise RuntimeError(f"MT5 account_info returned None: {self._bridge.last_error()}")

        account_id = self._account_id_from_info(info)
        if self.account_id is None or self.account_id != account_id:
            self._set_account_id(account_id)
            self._reset_reconciliation_mappings()

        currency = Currency.from_str(get_field(info, "currency", "USD"), strict=False)
        self.base_currency = currency
        locked = self._decimal_account_field(info, "margin")
        reported_free = self._optional_decimal_account_field(info, "margin_free")
        equity = self._optional_decimal_account_field(info, "equity")
        balance = self._optional_decimal_account_field(info, "balance")
        if equity is None:
            if reported_free is not None:
                equity = locked + reported_free
            else:
                equity = balance or Decimal(0)
        free = equity - locked

        self.generate_account_state(
            balances=[
                AccountBalance(
                    total=Money(equity, currency),
                    locked=Money(locked, currency),
                    free=Money(free, currency),
                ),
            ],
            margins=[
                MarginBalance(
                    initial=Money(max(locked, Decimal(0)), currency),
                    maintenance=Money(max(locked, Decimal(0)), currency),
                ),
            ],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info=self._account_info(info, equity=equity, free=free),
        )

    def _decimal_account_field(
        self,
        info: Any,
        name: str,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        value = get_field(info, name, None)
        if value is None:
            return default
        return Decimal(str(value))

    def _optional_decimal_account_field(self, info: Any, name: str) -> Decimal | None:
        value = get_field(info, name, None)
        if value is None:
            return None
        return Decimal(str(value))

    def _account_info(self, info: Any, *, equity: Decimal, free: Decimal) -> dict[str, Any]:
        values = dict(info._asdict()) if hasattr(info, "_asdict") else {}
        for name in (
            "login",
            "trade_mode",
            "leverage",
            "limit_orders",
            "margin_so_mode",
            "trade_allowed",
            "trade_expert",
            "margin_mode",
            "currency_digits",
            "fifo_close",
            "balance",
            "credit",
            "profit",
            "equity",
            "margin",
            "margin_free",
            "margin_level",
            "margin_so_call",
            "margin_so_so",
            "margin_initial",
            "margin_maintenance",
            "assets",
            "liabilities",
            "commission_blocked",
            "name",
            "server",
            "currency",
            "company",
        ):
            value = get_field(info, name, None)
            if value is not None:
                values[name] = value
        values["nautilus_equity"] = str(equity)
        values["nautilus_free_margin"] = str(free)
        values["nautilus_oms_type"] = oms_type_to_str(self._config.oms_type)
        return values

    def _reset_reconciliation_mappings(self) -> None:
        self._reconciliation_mappings = empty_reconciliation_payload()
        self._reconciliation_mappings_loaded = False

    def _reconciliation_cache_key(self) -> str | None:
        if self.account_id is None:
            return None
        return f"mt5:reconciliation:{self._client_id.value}:{self.account_id.value}:orders"

    def _load_reconciliation_mappings(self) -> None:
        if self._reconciliation_mappings_loaded or not self._config.persist_reconciliation_mappings:
            return

        cache_key = self._reconciliation_cache_key()
        if cache_key is None:
            return

        if getattr(self._cache, "has_backing", False):
            self._cache.cache_general()

        try:
            self._reconciliation_mappings = decode_reconciliation_payload(self._cache.get(cache_key))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._log.warning(f"Could not decode MT5 reconciliation cache {cache_key}: {e}")
            self._reconciliation_mappings = empty_reconciliation_payload()

        for comment, client_order_id in comment_mappings(self._reconciliation_mappings).items():
            self._client_order_ids_by_comment[comment] = ClientOrderId(client_order_id)

        for venue_order_id, client_order_id in venue_order_mappings(self._reconciliation_mappings).items():
            self._cache_venue_order_id(ClientOrderId(client_order_id), VenueOrderId(venue_order_id))

        self._reconciliation_mappings_loaded = True

    def _persist_reconciliation_mappings(self) -> None:
        if not self._config.persist_reconciliation_mappings:
            return
        cache_key = self._reconciliation_cache_key()
        if cache_key is None:
            return
        self._cache.add(cache_key, encode_reconciliation_payload(self._reconciliation_mappings))

    def _remember_order_mapping(
        self,
        client_order_id: ClientOrderId,
        *,
        comment: str | None = None,
        venue_order_id: VenueOrderId | str | int | None = None,
        deal_ticket: str | int | None = None,
        position_id: PositionId | str | int | None = None,
        ts_event: int | None = None,
    ) -> None:
        if comment:
            self._client_order_ids_by_comment[comment] = client_order_id
        if not self._reconciliation_mappings_loaded:
            self._load_reconciliation_mappings()

        raw_venue_order_id = venue_order_id.value if isinstance(venue_order_id, VenueOrderId) else venue_order_id
        raw_position_id = position_id.value if isinstance(position_id, PositionId) else position_id
        self._reconciliation_mappings = register_order_mapping(
            self._reconciliation_mappings,
            client_order_id=client_order_id.value,
            comment=comment,
            venue_order_id=raw_venue_order_id,
            deal_ticket=deal_ticket,
            position_id=raw_position_id,
            ts_event=ts_event,
        )
        self._persist_reconciliation_mappings()

    def _cache_venue_order_id(self, client_order_id: ClientOrderId, venue_order_id: VenueOrderId) -> None:
        try:
            self._cache.add_venue_order_id(client_order_id, venue_order_id, overwrite=True)
        except ValueError as e:
            self._log.warning(f"Could not cache MT5 venue order mapping {venue_order_id}: {e}")

    async def _monitor_terminal_health(self) -> None:
        interval_secs = self._config.terminal_health_check_interval_ms / 1000
        while True:
            try:
                if not await self._ensure_bridge_connected("execution terminal health monitor"):
                    self._log.warning(f"MT5 terminal health check failed: {self._bridge.last_error()}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(f"MT5 terminal health check error: {e}")
            await asyncio.sleep(interval_secs)

    async def _ensure_bridge_connected(self, reason: str) -> bool:
        connected = await self._bridge.ensure_connected(
            reconnect=self._config.reconnect_enabled,
            max_attempts=self._config.reconnect_max_attempts,
            initial_delay_ms=self._config.reconnect_initial_delay_ms,
            max_delay_ms=self._config.reconnect_max_delay_ms,
        )
        if not connected:
            self._log.warning(f"MT5 reconnect failed during {reason}")
        return connected

    async def _query_account(self, command: QueryAccount) -> None:
        await self._update_account_state()

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order
        if not await self._ensure_bridge_connected(f"submit order {order.client_order_id}"):
            self.generate_order_denied(
                command.strategy_id,
                command.instrument_id,
                order.client_order_id,
                "MT5_TERMINAL_UNAVAILABLE",
                self._clock.timestamp_ns(),
            )
            return
        instrument = await self._ensure_instrument(command.instrument_id)
        try:
            request = await self._order_to_mt5_request(command, instrument)
            await self._validate_request(command.instrument_id, request)
            if self._config.use_order_check:
                await self._check_order(command.instrument_id, request)
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

        if not await self._ensure_bridge_connected(f"modify order {command.client_order_id}"):
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                "MT5_TERMINAL_UNAVAILABLE",
                self._clock.timestamp_ns(),
            )
            return

        cached_order = self._cache.order(command.client_order_id)
        quantity = command.quantity or (cached_order.quantity if cached_order is not None else None)
        price = command.price or (cached_order.price if cached_order is not None and cached_order.has_price else None)
        trigger_price = command.trigger_price or (
            cached_order.trigger_price if cached_order is not None and cached_order.has_trigger_price else None
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

        if command.quantity is not None and (cached_order is None or command.quantity != cached_order.quantity):
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                "MT5_MODIFY_QUANTITY_UNSUPPORTED",
                self._clock.timestamp_ns(),
            )
            return

        request = self._modify_to_mt5_request(
            command,
            venue_order_id=venue_order_id,
            cached_order=cached_order,
            price=price,
            trigger_price=trigger_price,
        )

        result = await self._bridge.order_send(request)
        if get_field(result, "retcode") in self._success_retcodes():
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
        if not await self._ensure_bridge_connected(f"cancel order {command.client_order_id}"):
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                venue_order_id,
                "MT5_TERMINAL_UNAVAILABLE",
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
        if get_field(result, "retcode") in self._success_retcodes():
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
        if not await self._ensure_bridge_connected("cancel all orders"):
            return
        kwargs = {}
        if command.instrument_id is not None:
            kwargs["symbol"] = mt5_symbol_from_instrument_id(command.instrument_id)
        orders = await self._bridge.orders_get(**kwargs) or []
        for mt5_order in orders:
            if not self._is_own_record(mt5_order):
                continue
            client_order_id = self._client_order_id_from_comment(get_field(mt5_order, "comment"))
            if client_order_id is None:
                continue
            await self._cancel_order(
                CancelOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    instrument_id=instrument_id_from_mt5_symbol(get_field(mt5_order, "symbol")),
                    client_order_id=client_order_id,
                    venue_order_id=VenueOrderId(str(get_field(mt5_order, "ticket"))),
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                ),
            )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    async def _order_to_mt5_request(self, command: SubmitOrder, instrument: Any) -> dict[str, Any]:
        mt5 = self._bridge.mt5
        order = command.order
        symbol = mt5_symbol_from_instrument_id(command.instrument_id)
        await self._bridge.symbol_select(symbol, True)

        position = self._mt5_position_id(command.params.get("mt5_position_id") or command.position_id)
        position_by = self._mt5_position_id(command.params.get("mt5_position_by") or command.params.get("position_by"))
        request: dict[str, Any] = {
            "symbol": symbol,
            "volume": float(order.quantity.as_decimal()),
            "magic": self._config.magic,
            "deviation": self._config.deviation,
            "comment": self._order_comment(order.client_order_id),
        }
        self._apply_time_in_force(request, order)

        if position_by is not None:
            if position is None:
                raise ValueError("MT5_CLOSE_BY_REQUIRES_POSITION")
            request["action"] = mt5.TRADE_ACTION_CLOSE_BY
            request["position"] = position
            request["position_by"] = position_by
            return request

        if order.order_type == OrderType.MARKET:
            tick = await self._bridge.symbol_info_tick(symbol)
            if tick is None:
                raise RuntimeError(f"NO_TICK_FOR_SYMBOL: {symbol}")
            is_buy = order.side == OrderSide.BUY
            request["action"] = mt5.TRADE_ACTION_DEAL
            request["type"] = getattr(mt5, "ORDER_TYPE_BUY" if is_buy else "ORDER_TYPE_SELL")
            request["price"] = float(get_field(tick, "ask" if is_buy else "bid"))
            if position is not None:
                request["position"] = position
        elif order.order_type == OrderType.LIMIT:
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["type"] = getattr(mt5, "ORDER_TYPE_BUY_LIMIT" if order.side == OrderSide.BUY else "ORDER_TYPE_SELL_LIMIT")
            request["price"] = float(order.price)
        elif order.order_type == OrderType.STOP_MARKET:
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["type"] = getattr(mt5, "ORDER_TYPE_BUY_STOP" if order.side == OrderSide.BUY else "ORDER_TYPE_SELL_STOP")
            request["price"] = float(order.trigger_price)
        elif order.order_type == OrderType.STOP_LIMIT:
            request["action"] = mt5.TRADE_ACTION_PENDING
            request["type"] = getattr(
                mt5,
                "ORDER_TYPE_BUY_STOP_LIMIT" if order.side == OrderSide.BUY else "ORDER_TYPE_SELL_STOP_LIMIT",
            )
            request["price"] = float(order.trigger_price)
            request["stoplimit"] = float(order.price)
        else:
            raise ValueError(f"Unsupported MT5 order type: {order.order_type}")

        request["type_filling"] = self._resolve_filling_mode(symbol, request["action"], order.time_in_force)
        return request

    def _modify_to_mt5_request(
        self,
        command: ModifyOrder,
        *,
        venue_order_id: VenueOrderId,
        cached_order: Any,
        price: Any,
        trigger_price: Any,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "action": self._bridge.mt5.TRADE_ACTION_MODIFY,
            "order": int(venue_order_id.value),
            "magic": self._config.magic,
            "comment": self._order_comment(command.client_order_id),
        }
        order_type = cached_order.order_type if cached_order is not None else None
        if order_type == OrderType.STOP_LIMIT:
            if trigger_price is not None:
                request["price"] = float(trigger_price)
            if price is not None:
                request["stoplimit"] = float(price)
        elif order_type == OrderType.STOP_MARKET:
            if trigger_price is not None:
                request["price"] = float(trigger_price)
        else:
            if price is not None and trigger_price is not None:
                request["price"] = float(trigger_price)
                request["stoplimit"] = float(price)
            elif trigger_price is not None:
                request["price"] = float(trigger_price)
            elif price is not None:
                request["price"] = float(price)
        self._apply_modify_expiration(request, command.params)
        return request

    async def _validate_request(self, instrument_id: InstrumentId, request: dict[str, Any]) -> None:
        symbol = request["symbol"]
        rules = await self._rules_for_symbol(symbol)
        self._validate_trade_mode(rules)
        self._validate_volume(rules, Decimal(str(request["volume"])))
        if "price" in request:
            self._validate_stops_level(rules, request, await self._bridge.symbol_info_tick(symbol))

    async def _check_order(self, instrument_id: InstrumentId, request: dict[str, Any]) -> None:
        result = await self._bridge.order_check(request)
        if result is None:
            raise RuntimeError(f"MT5_ORDER_CHECK_NONE: {self._bridge.last_error()}")
        retcode = get_field(result, "retcode")
        if retcode not in self._success_retcodes():
            raise RuntimeError(f"MT5_ORDER_CHECK_FAILED: {self._retcode_reason(result)}")

    def _validate_trade_mode(self, rules: MT5SymbolRules) -> None:
        mt5 = self._bridge.mt5
        disabled = getattr(mt5, "SYMBOL_TRADE_MODE_DISABLED", None)
        close_only = getattr(mt5, "SYMBOL_TRADE_MODE_CLOSEONLY", None)
        if rules.trade_mode in (disabled, close_only):
            raise RuntimeError(f"MT5_SYMBOL_NOT_TRADABLE: {rules.symbol} trade_mode={rules.trade_mode}")

    def _validate_volume(self, rules: MT5SymbolRules, volume: Decimal) -> None:
        if rules.volume_min and volume < rules.volume_min:
            raise ValueError(f"MT5_VOLUME_BELOW_MIN: {volume} < {rules.volume_min}")
        if rules.volume_max and volume > rules.volume_max:
            raise ValueError(f"MT5_VOLUME_ABOVE_MAX: {volume} > {rules.volume_max}")
        if rules.volume_step > 0:
            steps = (volume / rules.volume_step).quantize(Decimal(1))
            if steps * rules.volume_step != volume:
                raise ValueError(f"MT5_VOLUME_STEP_MISMATCH: {volume} step={rules.volume_step}")

    def _validate_stops_level(self, rules: MT5SymbolRules, request: dict[str, Any], tick: Any) -> None:
        if rules.trade_stops_level <= 0 or tick is None:
            return
        mt5 = self._bridge.mt5
        order_type = request.get("type")
        buy_trigger_types = {
            mt5.ORDER_TYPE_BUY_LIMIT,
            mt5.ORDER_TYPE_BUY_STOP,
            mt5.ORDER_TYPE_BUY_STOP_LIMIT,
        }
        ref = get_field(tick, "ask") if order_type in buy_trigger_types else get_field(tick, "bid")
        if ref is None:
            return
        distance_points = abs(Decimal(str(request["price"])) - Decimal(str(ref))) / rules.point
        if distance_points < rules.trade_stops_level:
            raise ValueError(
                f"MT5_STOPS_LEVEL_TOO_CLOSE: distance={distance_points} required={rules.trade_stops_level}",
            )

    def _resolve_filling_mode(self, symbol: str, action: int, time_in_force: TimeInForce) -> int:
        mt5 = self._bridge.mt5
        if action != mt5.TRADE_ACTION_DEAL:
            return mt5.ORDER_FILLING_RETURN
        if time_in_force == TimeInForce.FOK:
            return mt5.ORDER_FILLING_FOK
        if time_in_force == TimeInForce.IOC:
            return mt5.ORDER_FILLING_IOC
        rules = self._instrument_provider.rules(symbol)
        filling_mode = rules.filling_mode if rules is not None else None
        for attr in ("ORDER_FILLING_IOC", "ORDER_FILLING_FOK", "ORDER_FILLING_RETURN"):
            value = getattr(mt5, attr, None)
            if value is not None and filling_mode == value:
                return value
        return mt5.ORDER_FILLING_IOC

    async def _rules_for_symbol(self, symbol: str) -> MT5SymbolRules:
        rules = self._instrument_provider.rules(symbol)
        if rules is not None:
            return rules
        info = await self._bridge.symbol_info(symbol)
        if info is None:
            raise RuntimeError(f"MT5_SYMBOL_INFO_NOT_FOUND: {symbol}")
        rules = parse_symbol_rules(info)
        self._instrument_provider._rules_by_symbol[symbol] = rules
        return rules

    async def _handle_submit_result(self, command: SubmitOrder, result: Any, instrument: Any) -> None:
        order = command.order
        if result is None:
            self._log.error(
                f"MT5 submit returned None for {order.client_order_id}: {self._bridge.last_error()}. "
                "Leaving order in-flight for reconciliation.",
            )
            return
        if get_field(result, "retcode") not in self._success_retcodes():
            self.generate_order_rejected(
                command.strategy_id,
                command.instrument_id,
                order.client_order_id,
                self._retcode_reason(result),
                self._clock.timestamp_ns(),
            )
            return
        venue_order_id = VenueOrderId(str(get_field(result, "order") or get_field(result, "deal")))
        deal = get_field(result, "deal", 0) or 0
        position_id = get_field(result, "position", 0) or 0
        self._cache_venue_order_id(order.client_order_id, venue_order_id)
        self._remember_order_mapping(
            order.client_order_id,
            comment=self._order_comment(order.client_order_id),
            venue_order_id=venue_order_id,
            deal_ticket=deal,
            position_id=position_id,
            ts_event=self._clock.timestamp_ns(),
        )
        self.generate_order_accepted(
            command.strategy_id,
            command.instrument_id,
            order.client_order_id,
            venue_order_id,
            self._clock.timestamp_ns(),
        )
        if deal:
            self.generate_order_filled(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=TradeId(str(deal)),
                order_side=order.side,
                order_type=order.order_type,
                last_qty=instrument.make_qty(get_field(result, "volume") or order.quantity.as_decimal()),
                last_px=instrument.make_price(get_field(result, "price")),
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
        name = self._retcode_name(retcode)
        comment = get_field(result, "comment", "")
        return f"{name} ({retcode}): {comment}"

    def _retcode_name(self, retcode: Any) -> str:
        if not isinstance(retcode, int):
            return "MT5_RETCODE_UNKNOWN"
        mt5 = self._bridge.mt5
        names = (
            "TRADE_RETCODE_REQUOTE",
            "TRADE_RETCODE_REJECT",
            "TRADE_RETCODE_CANCEL",
            "TRADE_RETCODE_PLACED",
            "TRADE_RETCODE_DONE",
            "TRADE_RETCODE_DONE_PARTIAL",
            "TRADE_RETCODE_ERROR",
            "TRADE_RETCODE_TIMEOUT",
            "TRADE_RETCODE_INVALID",
            "TRADE_RETCODE_INVALID_VOLUME",
            "TRADE_RETCODE_INVALID_PRICE",
            "TRADE_RETCODE_INVALID_STOPS",
            "TRADE_RETCODE_TRADE_DISABLED",
            "TRADE_RETCODE_MARKET_CLOSED",
            "TRADE_RETCODE_NO_MONEY",
            "TRADE_RETCODE_PRICE_CHANGED",
            "TRADE_RETCODE_PRICE_OFF",
            "TRADE_RETCODE_INVALID_EXPIRATION",
            "TRADE_RETCODE_ORDER_CHANGED",
            "TRADE_RETCODE_TOO_MANY_REQUESTS",
            "TRADE_RETCODE_NO_CHANGES",
            "TRADE_RETCODE_SERVER_DISABLES_AT",
            "TRADE_RETCODE_CLIENT_DISABLES_AT",
            "TRADE_RETCODE_LOCKED",
            "TRADE_RETCODE_FROZEN",
            "TRADE_RETCODE_INVALID_FILL",
            "TRADE_RETCODE_CONNECTION",
            "TRADE_RETCODE_ONLY_REAL",
            "TRADE_RETCODE_LIMIT_ORDERS",
            "TRADE_RETCODE_LIMIT_VOLUME",
            "TRADE_RETCODE_INVALID_ORDER",
            "TRADE_RETCODE_POSITION_CLOSED",
            "TRADE_RETCODE_INVALID_CLOSE_VOLUME",
            "TRADE_RETCODE_CLOSE_ORDER_EXIST",
            "TRADE_RETCODE_LIMIT_POSITIONS",
            "TRADE_RETCODE_REJECT_CANCEL",
            "TRADE_RETCODE_LONG_ONLY",
            "TRADE_RETCODE_SHORT_ONLY",
            "TRADE_RETCODE_CLOSE_ONLY",
            "TRADE_RETCODE_FIFO_CLOSE",
        )
        for name in names:
            if getattr(mt5, name, None) == retcode:
                return name
        return f"MT5_RETCODE_{retcode}"

    def _apply_time_in_force(self, request: dict[str, Any], order: Any) -> None:
        mt5 = self._bridge.mt5
        if order.time_in_force == TimeInForce.DAY:
            request["type_time"] = mt5.ORDER_TIME_DAY
        elif order.time_in_force == TimeInForce.GTD:
            expire_time_ns = getattr(order, "expire_time_ns", 0) or 0
            if expire_time_ns <= 0:
                raise ValueError("MT5_GTD_REQUIRES_EXPIRE_TIME")
            request["type_time"] = mt5.ORDER_TIME_SPECIFIED
            request["expiration"] = utc_from_ns(expire_time_ns)
        else:
            request["type_time"] = mt5.ORDER_TIME_GTC

    def _apply_modify_expiration(self, request: dict[str, Any], params: dict[str, object]) -> None:
        expire_time_ns = params.get("expire_time_ns")
        if expire_time_ns is None:
            return
        mt5 = self._bridge.mt5
        expiration_ns = int(str(expire_time_ns))
        if expiration_ns <= 0:
            request["type_time"] = mt5.ORDER_TIME_GTC
            return
        request["type_time"] = mt5.ORDER_TIME_SPECIFIED
        request["expiration"] = utc_from_ns(expiration_ns)

    def _mt5_position_id(self, value: object | None) -> int | None:
        if value is None:
            return None
        raw = value.value if isinstance(value, PositionId) else str(value)
        if not raw.isdigit():
            raise ValueError(f"MT5_POSITION_ID_NOT_NUMERIC: {raw}")
        return int(raw)

    def _default_history_start(self) -> datetime:
        return datetime.now(tz=UTC) - timedelta(days=self._config.reconciliation_lookback_days)

    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None:
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

    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]:
        if not await self._ensure_bridge_connected("generate order status reports"):
            return []
        reports: list[OrderStatusReport] = []
        symbol = mt5_symbol_from_instrument_id(command.instrument_id) if command.instrument_id else None
        active_orders = await self._bridge.orders_get(**({"symbol": symbol} if symbol else {})) or []
        for mt5_order in active_orders:
            if self._is_own_record(mt5_order):
                reports.append(await self._parse_order_status_report(mt5_order, active=True))
        if not command.open_only:
            start = command.start or self._default_history_start()
            end = command.end or datetime.now(tz=UTC)
            history = await self._bridge.history_orders_get(start, end, **({"symbol": symbol} if symbol else {})) or []
            for mt5_order in history:
                if self._is_own_record(mt5_order):
                    reports.append(await self._parse_order_status_report(mt5_order, active=False))
        return reports

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        if not await self._ensure_bridge_connected("generate fill reports"):
            return []
        symbol = mt5_symbol_from_instrument_id(command.instrument_id) if command.instrument_id else None
        start = command.start or self._default_history_start()
        end = command.end or datetime.now(tz=UTC)
        deals = await self._bridge.history_deals_get(start, end, **({"symbol": symbol} if symbol else {})) or []
        reports = []
        for deal in deals:
            if not self._is_own_record(deal):
                continue
            if command.venue_order_id and str(get_field(deal, "order")) != command.venue_order_id.value:
                continue
            report = await self._parse_fill_report(deal)
            if report is not None:
                reports.append(report)
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        if not await self._ensure_bridge_connected("generate position status reports"):
            return []
        symbol = mt5_symbol_from_instrument_id(command.instrument_id) if command.instrument_id else None
        positions = await self._bridge.positions_get(**({"symbol": symbol} if symbol else {})) or []
        reports = []
        for position in positions:
            if self._include_position_report(position):
                report = await self._parse_position_status_report(position)
                if report is not None:
                    reports.append(report)
        return reports

    def _client_order_id_from_mt5_mapping(
        self,
        *,
        comment: str | None = None,
        venue_order_id: str | int | None = None,
        deal_ticket: str | int | None = None,
    ) -> ClientOrderId | None:
        client_order_id = self._client_order_id_from_comment(comment)
        if client_order_id is not None:
            return client_order_id

        if not self._reconciliation_mappings_loaded:
            self._load_reconciliation_mappings()

        mapped = (
            client_order_id_from_venue_order_id(self._reconciliation_mappings, venue_order_id)
            or client_order_id_from_deal_ticket(self._reconciliation_mappings, deal_ticket)
        )
        return ClientOrderId(mapped) if mapped else None

    async def _parse_order_status_report(self, mt5_order: Any, *, active: bool) -> OrderStatusReport:
        mt5 = self._bridge.mt5
        instrument_id = instrument_id_from_mt5_symbol(get_field(mt5_order, "symbol"))
        instrument = await self._ensure_instrument(instrument_id)
        raw_quantity = get_field(mt5_order, "volume_initial") or get_field(mt5_order, "volume_current") or 0
        if Decimal(str(raw_quantity)) <= 0:
            raise ValueError(f"MT5 order {get_field(mt5_order, 'ticket')} has non-positive quantity")
        quantity = instrument.make_qty(raw_quantity)
        remaining = instrument.make_qty(get_field(mt5_order, "volume_current") or 0)
        price = get_field(mt5_order, "price_open", 0) or 0
        position_id = get_field(mt5_order, "position_id", 0) or 0
        ticket = get_field(mt5_order, "ticket")
        comment = get_field(mt5_order, "comment")
        client_order_id = self._client_order_id_from_mt5_mapping(comment=comment, venue_order_id=ticket)
        venue_order_id = VenueOrderId(str(ticket))
        if client_order_id is not None:
            self._cache_venue_order_id(client_order_id, venue_order_id)
            self._remember_order_mapping(
                client_order_id,
                comment=comment,
                venue_order_id=venue_order_id,
                position_id=position_id,
                ts_event=to_unix_nanos(
                    get_field(mt5_order, "time_done_msc", None)
                    or get_field(mt5_order, "time_done", None)
                    or get_field(mt5_order, "time_setup"),
                ),
            )
        return OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=PositionId(str(position_id)) if position_id else None,
            order_side=order_side_from_mt5(mt5, get_field(mt5_order, "type")),
            order_type=order_type_from_mt5(mt5, get_field(mt5_order, "type")),
            time_in_force=TimeInForce.GTC,
            order_status=order_status_from_mt5(mt5, get_field(mt5_order, "state"), active=active),
            quantity=quantity,
            filled_qty=quantity.saturating_sub(remaining),
            price=instrument.make_price(price) if price else None,
            avg_px=Decimal(str(get_field(mt5_order, "price_current", 0) or price or 0)),
            report_id=UUID4(),
            ts_accepted=to_unix_nanos(get_field(mt5_order, "time_setup_msc", None) or get_field(mt5_order, "time_setup")),
            ts_last=to_unix_nanos(
                get_field(mt5_order, "time_done_msc", None)
                or get_field(mt5_order, "time_done", None)
                or get_field(mt5_order, "time_setup"),
            ),
            ts_init=self._clock.timestamp_ns(),
        )

    async def _parse_fill_report(self, deal: Any) -> FillReport | None:
        volume = get_field(deal, "volume", 0) or 0
        if volume <= 0:
            return None
        instrument_id = instrument_id_from_mt5_symbol(get_field(deal, "symbol"))
        instrument = await self._ensure_instrument(instrument_id)
        mt5_type = get_field(deal, "type")
        side = OrderSide.SELL if mt5_type == getattr(self._bridge.mt5, "DEAL_TYPE_SELL", 1) else OrderSide.BUY
        commission = Decimal(str(get_field(deal, "commission", 0) or 0)).copy_abs()
        position_id = get_field(deal, "position_id", 0) or 0
        order_ticket = get_field(deal, "order")
        deal_ticket = get_field(deal, "ticket")
        comment = get_field(deal, "comment")
        client_order_id = self._client_order_id_from_mt5_mapping(
            comment=comment,
            venue_order_id=order_ticket,
            deal_ticket=deal_ticket,
        )
        venue_order_id = VenueOrderId(str(order_ticket))
        if client_order_id is not None:
            self._cache_venue_order_id(client_order_id, venue_order_id)
            self._remember_order_mapping(
                client_order_id,
                comment=comment,
                venue_order_id=venue_order_id,
                deal_ticket=deal_ticket,
                position_id=position_id,
                ts_event=to_unix_nanos(get_field(deal, "time_msc", None) or get_field(deal, "time")),
            )
        return FillReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=PositionId(str(position_id)) if position_id else None,
            trade_id=TradeId(str(deal_ticket)),
            order_side=side,
            last_qty=instrument.make_qty(volume),
            last_px=instrument.make_price(get_field(deal, "price")),
            commission=Money(commission, instrument.quote_currency),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            report_id=UUID4(),
            ts_event=to_unix_nanos(get_field(deal, "time_msc", None) or get_field(deal, "time")),
            ts_init=self._clock.timestamp_ns(),
        )

    async def _parse_position_status_report(self, position: Any) -> PositionStatusReport | None:
        volume = get_field(position, "volume", 0) or 0
        if volume <= 0:
            return None
        instrument_id = instrument_id_from_mt5_symbol(get_field(position, "symbol"))
        instrument = await self._ensure_instrument(instrument_id)
        position_id = self._position_id(position)
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
            ts_last=to_unix_nanos(
                get_field(position, "time_update_msc", None)
                or get_field(position, "time_update", None)
                or get_field(position, "time"),
            ),
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
        return get_field(record, "magic", None) == self._config.magic

    def _include_position_report(self, position: Any) -> bool:
        if not self._config.filter_position_reports_by_magic:
            return True
        return self._is_own_record(position)

    def _position_id(self, position: Any) -> Any:
        ticket = get_field(position, "ticket", 0) or 0
        identifier = get_field(position, "identifier", 0) or 0
        if self._config.oms_type == OmsType.HEDGING:
            return ticket or identifier
        return identifier or ticket

    def _order_comment(self, client_order_id: ClientOrderId) -> str:
        value = client_order_id.value
        if len(value) <= 31:
            comment = value
        else:
            comment = f"NT:{hashlib.blake2s(value.encode('utf-8'), digest_size=14).hexdigest()}"
        self._remember_order_mapping(client_order_id, comment=comment, ts_event=self._clock.timestamp_ns())
        return comment

    def _client_order_id_from_comment(self, comment: str | None) -> ClientOrderId | None:
        if not comment:
            return None
        mapped = self._client_order_ids_by_comment.get(comment)
        if mapped is not None:
            return mapped
        if not self._reconciliation_mappings_loaded:
            self._load_reconciliation_mappings()
        mapped_value = client_order_id_from_comment(self._reconciliation_mappings, comment)
        if mapped_value is not None:
            mapped = ClientOrderId(mapped_value)
            self._client_order_ids_by_comment[comment] = mapped
            return mapped
        if comment.startswith("NT:"):
            return None
        parsed = parse_client_order_id(comment)
        if parsed is not None:
            self._remember_order_mapping(parsed, comment=comment, ts_event=self._clock.timestamp_ns())
        return parsed
