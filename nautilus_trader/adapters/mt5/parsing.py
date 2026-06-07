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
Parsing helpers for the MetaTrader 5 adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_trader.adapters.mt5.symbol import infer_fx_currencies
from nautilus_trader.adapters.mt5.symbol import instrument_id_from_mt5_symbol
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import Cfd
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


@dataclass(frozen=True)
class MT5SymbolRules:
    """
    Broker-specific trading rules derived from MT5 ``symbol_info``.
    """

    symbol: str
    trade_mode: int | None
    filling_mode: int | None
    order_mode: int | None
    volume_min: Decimal
    volume_max: Decimal
    volume_step: Decimal
    trade_stops_level: int
    trade_freeze_level: int
    point: Decimal


def decimal_precision(value: Any) -> int:
    decimal = Decimal(str(value))
    if decimal == 0:
        return 0
    exponent = decimal.normalize().as_tuple().exponent
    if not isinstance(exponent, int):
        return 0
    return max(0, -exponent)


def format_decimal(value: Any, precision: int) -> str:
    return f"{Decimal(str(value)):.{precision}f}"


def get_field(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    if hasattr(obj, name):
        return getattr(obj, name)
    try:
        return obj[name]
    except (KeyError, TypeError, IndexError):
        return default


def mt5_time_to_nanos(obj: Any, *fields: str) -> int:
    """
    Resolve event time (UNIX nanoseconds) from MT5 record fields.

    MT5 records expose times with known units: ``<field>`` is in seconds while the
    sibling ``<field>_msc`` is in milliseconds. For each field the millisecond sibling
    is preferred, then the seconds field. Returns 0 if none are present.
    """
    for field in fields:
        msc = get_field(obj, f"{field}_msc", None)
        if msc:
            return int(msc) * 1_000_000
        secs = get_field(obj, field, None)
        if secs:
            return int(secs) * 1_000_000_000
    return 0


def to_unix_nanos(value: Any) -> int:
    """
    Convert a raw MT5 time value to UNIX nanoseconds.

    Prefer :func:`mt5_time_to_nanos` when the source record exposes the field name, as it
    disambiguates seconds from milliseconds by field rather than by magnitude.
    """
    if value is None:
        return 0
    if isinstance(value, int | float):
        if value > 10_000_000_000:
            return int(value) * 1_000_000
        return int(value) * 1_000_000_000
    return int(pd.Timestamp(value).tz_convert("UTC").value)


def utc_now_ns() -> int:
    return int(pd.Timestamp.now(tz="UTC").value)


def utc_from_ns(ns: int) -> datetime:
    return pd.Timestamp(ns, unit="ns", tz="UTC").to_pydatetime()


def parse_symbol_rules(info: Any) -> MT5SymbolRules:
    """
    Parse MT5 broker trading rules from ``symbol_info``.
    """
    symbol = get_field(info, "name")
    if not symbol:
        raise ValueError("MT5 symbol_info had no name")
    digits = int(get_field(info, "digits", 5) or 5)
    point = Decimal(str(get_field(info, "point", Decimal(10) ** -digits)))
    return MT5SymbolRules(
        symbol=symbol,
        trade_mode=get_field(info, "trade_mode", None),
        filling_mode=get_field(info, "filling_mode", None),
        order_mode=get_field(info, "order_mode", None),
        volume_min=Decimal(str(get_field(info, "volume_min", 0) or 0)),
        volume_max=Decimal(str(get_field(info, "volume_max", 0) or 0)),
        volume_step=Decimal(str(get_field(info, "volume_step", 1) or 1)),
        trade_stops_level=int(get_field(info, "trade_stops_level", 0) or 0),
        trade_freeze_level=int(get_field(info, "trade_freeze_level", 0) or 0),
        point=point,
    )


def parse_instrument(
    info: Any,
    *,
    symbol_suffixes: list[str] | None = None,
    default_asset_class: str = "FX",
    ts_init: int | None = None,
) -> CurrencyPair | Cfd:
    """
    Convert an MT5 ``symbol_info`` object to a Nautilus instrument.
    """
    symbol = get_field(info, "name")
    if not symbol:
        raise ValueError("MT5 symbol_info had no name")

    instrument_id = instrument_id_from_mt5_symbol(symbol)
    raw_symbol = Symbol(symbol)

    digits = int(get_field(info, "digits", 5) or 5)
    point = get_field(info, "point", Decimal(10) ** -digits)
    volume_step = get_field(info, "volume_step", 1) or 1
    volume_min = get_field(info, "volume_min", 0) or 0
    volume_max = get_field(info, "volume_max", 0) or 0
    contract_size = get_field(info, "trade_contract_size", 1) or 1
    currency_profit = get_field(info, "currency_profit", None)
    currency_base = get_field(info, "currency_base", None)

    size_precision = decimal_precision(volume_step)
    price_increment = Price.from_str(format_decimal(point, digits))
    size_increment = Quantity.from_str(format_decimal(volume_step, size_precision))
    min_quantity = (
        Quantity.from_str(format_decimal(volume_min, size_precision)) if volume_min else None
    )
    max_quantity = (
        Quantity.from_str(format_decimal(volume_max, size_precision)) if volume_max else None
    )
    lot_size = Quantity.from_str(format_decimal(1, size_precision))
    multiplier = Quantity.from_str(format_decimal(contract_size, decimal_precision(contract_size)))
    ts = ts_init if ts_init is not None else utc_now_ns()
    info_dict = dict(info._asdict()) if hasattr(info, "_asdict") else {"symbol": symbol}
    info_dict["mt5_rules"] = parse_symbol_rules(info).__dict__

    fx = infer_fx_currencies(symbol, symbol_suffixes)
    if fx is not None:
        base, quote = fx
        return CurrencyPair(
            instrument_id=instrument_id,
            raw_symbol=raw_symbol,
            base_currency=Currency.from_str(currency_base or base),
            quote_currency=Currency.from_str(currency_profit or quote),
            price_precision=digits,
            size_precision=size_precision,
            price_increment=price_increment,
            size_increment=size_increment,
            multiplier=multiplier,
            lot_size=lot_size,
            min_quantity=min_quantity,
            max_quantity=max_quantity,
            margin_init=Decimal(0),
            margin_maint=Decimal(0),
            maker_fee=Decimal(0),
            taker_fee=Decimal(0),
            ts_event=ts,
            ts_init=ts,
            info=info_dict,
        )

    quote_currency = Currency.from_str(currency_profit or "USD", strict=False)
    asset_class = getattr(AssetClass, default_asset_class, AssetClass.FX)
    return Cfd(
        instrument_id=instrument_id,
        raw_symbol=raw_symbol,
        asset_class=asset_class,
        quote_currency=quote_currency,
        price_precision=digits,
        size_precision=size_precision,
        price_increment=price_increment,
        size_increment=size_increment,
        lot_size=lot_size,
        min_quantity=min_quantity,
        max_quantity=max_quantity,
        margin_init=Decimal(0),
        margin_maint=Decimal(0),
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        ts_event=ts,
        ts_init=ts,
        info=info_dict,
    )


def parse_quote_tick(
    instrument_id: InstrumentId,
    tick: Any,
    instrument: Any,
    *,
    default_size: float,
    ts_init: int | None = None,
) -> QuoteTick | None:
    bid = get_field(tick, "bid", 0) or 0
    ask = get_field(tick, "ask", 0) or 0
    if bid <= 0 or ask <= 0:
        return None

    ts_event = mt5_time_to_nanos(tick, "time")
    ts = ts_init if ts_init is not None else utc_now_ns()
    size = instrument.make_qty(default_size)

    return QuoteTick(
        instrument_id=instrument_id,
        bid_price=instrument.make_price(bid),
        ask_price=instrument.make_price(ask),
        bid_size=size,
        ask_size=size,
        ts_event=ts_event,
        ts_init=ts,
    )


def mt5_timeframe(mt5: Any, bar_type: BarType) -> int:
    """
    Map a Nautilus time bar type to an MT5 timeframe constant.
    """
    step = bar_type.spec.step
    aggregation = bar_type.spec.aggregation

    mapping = {
        (BarAggregation.MINUTE, 1): "TIMEFRAME_M1",
        (BarAggregation.MINUTE, 2): "TIMEFRAME_M2",
        (BarAggregation.MINUTE, 3): "TIMEFRAME_M3",
        (BarAggregation.MINUTE, 4): "TIMEFRAME_M4",
        (BarAggregation.MINUTE, 5): "TIMEFRAME_M5",
        (BarAggregation.MINUTE, 6): "TIMEFRAME_M6",
        (BarAggregation.MINUTE, 10): "TIMEFRAME_M10",
        (BarAggregation.MINUTE, 12): "TIMEFRAME_M12",
        (BarAggregation.MINUTE, 15): "TIMEFRAME_M15",
        (BarAggregation.MINUTE, 20): "TIMEFRAME_M20",
        (BarAggregation.MINUTE, 30): "TIMEFRAME_M30",
        (BarAggregation.HOUR, 1): "TIMEFRAME_H1",
        (BarAggregation.HOUR, 2): "TIMEFRAME_H2",
        (BarAggregation.HOUR, 3): "TIMEFRAME_H3",
        (BarAggregation.HOUR, 4): "TIMEFRAME_H4",
        (BarAggregation.HOUR, 6): "TIMEFRAME_H6",
        (BarAggregation.HOUR, 8): "TIMEFRAME_H8",
        (BarAggregation.HOUR, 12): "TIMEFRAME_H12",
        (BarAggregation.DAY, 1): "TIMEFRAME_D1",
        (BarAggregation.WEEK, 1): "TIMEFRAME_W1",
        (BarAggregation.MONTH, 1): "TIMEFRAME_MN1",
    }
    attr = mapping.get((aggregation, step))
    if attr is None:
        raise ValueError(f"Unsupported MT5 bar timeframe: {bar_type}")

    return getattr(mt5, attr)


def parse_bar(bar_type: BarType, row: Any, instrument: Any, *, ts_init: int | None = None) -> Bar:
    ts_event = mt5_time_to_nanos(row, "time")
    ts = ts_init if ts_init is not None else utc_now_ns()
    volume = get_field(row, "real_volume", 0) or get_field(row, "tick_volume", 0) or 1

    return Bar(
        bar_type=bar_type,
        open=instrument.make_price(get_field(row, "open")),
        high=instrument.make_price(get_field(row, "high")),
        low=instrument.make_price(get_field(row, "low")),
        close=instrument.make_price(get_field(row, "close")),
        volume=instrument.make_qty(volume),
        ts_event=ts_event,
        ts_init=ts,
    )


def order_side_from_mt5(mt5: Any, mt5_type: int) -> OrderSide:
    sell_types = {
        getattr(mt5, "ORDER_TYPE_SELL", -1),
        getattr(mt5, "ORDER_TYPE_SELL_LIMIT", -1),
        getattr(mt5, "ORDER_TYPE_SELL_STOP", -1),
        getattr(mt5, "ORDER_TYPE_SELL_STOP_LIMIT", -1),
    }
    return OrderSide.SELL if mt5_type in sell_types else OrderSide.BUY


def order_type_from_mt5(mt5: Any, mt5_type: int) -> OrderType:
    if mt5_type in (getattr(mt5, "ORDER_TYPE_BUY_LIMIT", -1), getattr(mt5, "ORDER_TYPE_SELL_LIMIT", -1)):
        return OrderType.LIMIT
    if mt5_type in (getattr(mt5, "ORDER_TYPE_BUY_STOP", -1), getattr(mt5, "ORDER_TYPE_SELL_STOP", -1)):
        return OrderType.STOP_MARKET
    if mt5_type in (
        getattr(mt5, "ORDER_TYPE_BUY_STOP_LIMIT", -1),
        getattr(mt5, "ORDER_TYPE_SELL_STOP_LIMIT", -1),
    ):
        return OrderType.STOP_LIMIT
    return OrderType.MARKET


def order_status_from_mt5(mt5: Any, state: int | None, *, active: bool = False) -> OrderStatus:
    if active:
        return OrderStatus.ACCEPTED
    if state == getattr(mt5, "ORDER_STATE_CANCELED", None):
        return OrderStatus.CANCELED
    if state == getattr(mt5, "ORDER_STATE_EXPIRED", None):
        return OrderStatus.EXPIRED
    if state == getattr(mt5, "ORDER_STATE_REJECTED", None):
        return OrderStatus.REJECTED
    if state in (
        getattr(mt5, "ORDER_STATE_FILLED", None),
        getattr(mt5, "ORDER_STATE_PARTIAL", None),
    ):
        return OrderStatus.FILLED
    return OrderStatus.ACCEPTED


def parse_client_order_id(comment: str | None) -> ClientOrderId | None:
    if not comment:
        return None
    token = comment.split("|", 1)[0].strip()
    if not token:
        return None
    try:
        return ClientOrderId(token)
    except Exception:
        return None


def market_filling_mode(mt5: Any, instrument: Any) -> int:
    """
    Resolve the order filling mode for a market order from the symbol's allowed modes.

    Brokers advertise the supported filling policies through ``symbol_info.filling_mode``
    (a bitmask of ``SYMBOL_FILLING_FOK`` / ``SYMBOL_FILLING_IOC``), stored on the
    instrument ``info``. IOC is preferred, then FOK. When the symbol advertises neither
    (or the field is missing), IOC is used as the fallback, since ``ORDER_FILLING_RETURN``
    is generally rejected for an immediate market deal.
    """
    info = getattr(instrument, "info", None)
    filling = int(get_field(info, "filling_mode", 0) or 0)
    if filling & getattr(mt5, "SYMBOL_FILLING_FOK", 1) and not filling & getattr(
        mt5,
        "SYMBOL_FILLING_IOC",
        2,
    ):
        return mt5.ORDER_FILLING_FOK
    return mt5.ORDER_FILLING_IOC


def mt5_position_side(mt5: Any, position_type: int) -> PositionSide:
    if position_type == getattr(mt5, "POSITION_TYPE_SELL", 1):
        return PositionSide.SHORT
    return PositionSide.LONG


def zero_money(currency: Currency) -> Money:
    return Money(Decimal(0), currency)
