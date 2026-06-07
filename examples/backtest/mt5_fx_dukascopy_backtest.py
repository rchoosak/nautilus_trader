#!/usr/bin/env python3
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
Offline MT5 Forex backtest from Dukascopy data (runs on macOS, no MetaTrader5 required).

By default this runs out-of-the-box against bundled sample tick data. To use real Dukascopy
data, set one of the following environment variables to a local file (CSV or Parquet):

    MT5_DUKASCOPY_TICKS=/path/to/EURUSD_ticks.csv   # Gmt time, Ask, Bid, AskVolume, BidVolume
    MT5_DUKASCOPY_BARS=/path/to/EURUSD_m1.csv       # Gmt time, Open, High, Low, Close, Volume

Optionally set MT5_SYMBOL (default EURUSD when a file is supplied, AUDUSD for the sample).
Metals such as XAUUSD are supported automatically; for any other CFD set MT5_DIGITS and
MT5_CONTRACT_SIZE to match the broker's contract.

Account and strategy sizing can also be overridden via environment variables:

    MT5_BALANCE=25000        # starting balance amount (default 100000)
    MT5_CURRENCY=EUR         # account base currency (default USD)
    MT5_TRADE_SIZE=0.05      # fixed trade size in lots (default 0.10)

In risk mode the strategy places a real stop-loss at MT5_RISK_STOP_PIPS and recomputes the
lot size on every entry from the current account equity:

    MT5_TRADE_SIZE=risk:1%   # risk 1% of current equity per trade
    MT5_RISK_STOP_PIPS=50    # stop-loss distance in pips (default 50)
    MT5_TP_RR=2              # optional take-profit as a risk-reward multiple (default 0 = none)
"""

import os
from decimal import Decimal

import pandas as pd

from nautilus_trader.adapters.mt5 import MT5_VENUE
from nautilus_trader.adapters.mt5 import load_dukascopy_bars
from nautilus_trader.adapters.mt5 import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5 import mt5_fx_instrument
from nautilus_trader.adapters.mt5 import parse_trade_size_spec
from nautilus_trader.adapters.mt5 import risk_based_lots
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestDataProvider
from nautilus_trader.trading.strategy import Strategy


class MT5RiskEMACrossConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``MT5RiskEMACross``.
    """

    instrument_id: InstrumentId
    bar_type: BarType
    fast_ema_period: int = 10
    slow_ema_period: int = 20
    stop_pips: float = 50.0
    risk_pct: float | None = None
    fixed_size: Decimal | None = None
    account_currency: str = "USD"
    tp_rr: float = 0.0


class MT5RiskEMACross(Strategy):
    """
    EMA-cross strategy that enters at market with a real protective stop-loss.

    On every entry the position size is taken from ``fixed_size``, or — when ``risk_pct`` is
    set — recomputed from the current account equity so that the stop risks ``risk_pct`` of
    equity. Positions are flipped on the opposite EMA cross.
    """

    def __init__(self, config: MT5RiskEMACrossConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)
        self._pip_size = 0.0

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument {self.config.instrument_id}")
            self.stop()
            return

        self._pip_size = float(self.instrument.price_increment) * 10.0
        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        if not self.indicators_initialized() or bar.is_single_price():
            return

        instrument_id = self.config.instrument_id
        if self.fast_ema.value >= self.slow_ema.value:
            if self.portfolio.is_flat(instrument_id):
                self._enter(OrderSide.BUY, bar)
            elif self.portfolio.is_net_short(instrument_id):
                self.close_all_positions(instrument_id)
                self._enter(OrderSide.BUY, bar)
        elif self.portfolio.is_flat(instrument_id):
            self._enter(OrderSide.SELL, bar)
        elif self.portfolio.is_net_long(instrument_id):
            self.close_all_positions(instrument_id)
            self._enter(OrderSide.SELL, bar)

    def _enter(self, side: OrderSide, bar: Bar) -> None:
        if self.instrument is None:
            return

        self.cancel_all_orders(self.config.instrument_id)

        quantity = self.instrument.make_qty(self._resolve_size())
        stop_distance = self.config.stop_pips * self._pip_size
        close = float(bar.close)

        # When tp_rr <= 0 the take-profit is effectively disabled by placing it far away
        # (and always positive); the position then exits on the opposite EMA cross or the SL.
        if side == OrderSide.BUY:
            sl_price = close - stop_distance
            tp_price = close + stop_distance * self.config.tp_rr if self.config.tp_rr > 0 else close * 2.0
        else:
            sl_price = close + stop_distance
            tp_price = close - stop_distance * self.config.tp_rr if self.config.tp_rr > 0 else close * 0.5

        order_list = self.order_factory.bracket(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            entry_order_type=OrderType.MARKET,
            time_in_force=TimeInForce.GTC,
            sl_trigger_price=self.instrument.make_price(sl_price),
            tp_price=self.instrument.make_price(tp_price),
        )
        self.submit_order_list(order_list)

    def _resolve_size(self) -> Decimal:
        config = self.config
        if config.risk_pct is not None:
            account = self.portfolio.account(config.instrument_id.venue)
            if account is not None:
                equity = account.balance_total(
                    Currency.from_str(config.account_currency, strict=False),
                )
                if equity is not None:
                    return risk_based_lots(
                        equity.as_double(),
                        risk_pct=config.risk_pct,
                        stop_pips=config.stop_pips,
                        instrument=self.instrument,
                    )
        if config.fixed_size is not None:
            return config.fixed_size
        if self.instrument is not None and self.instrument.min_quantity is not None:
            return self.instrument.min_quantity.as_decimal()
        return Decimal("0.01")

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.bar_type)

    def on_reset(self) -> None:
        self.fast_ema.reset()
        self.slow_ema.reset()


def _make_instrument(symbol: str):
    # Metals (e.g. XAUUSD) are auto-detected; MT5_DIGITS/MT5_CONTRACT_SIZE override for any
    # other CFD the broker offers (live trading reads these from the terminal instead).
    digits = os.getenv("MT5_DIGITS")
    contract_size = os.getenv("MT5_CONTRACT_SIZE")
    return mt5_fx_instrument(
        symbol,
        digits=int(digits) if digits else None,
        contract_size=float(contract_size) if contract_size else None,
    )


def _build_data(engine: BacktestEngine):
    ticks_file = os.getenv("MT5_DUKASCOPY_TICKS")
    bars_file = os.getenv("MT5_DUKASCOPY_BARS")

    if bars_file:
        symbol = os.getenv("MT5_SYMBOL", "EURUSD")
        instrument = _make_instrument(symbol)
        engine.add_instrument(instrument)
        bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-BID-EXTERNAL")
        bars = load_dukascopy_bars(bars_file, bar_type=bar_type, instrument=instrument)
        engine.add_data(bars)
        return instrument, bar_type

    if ticks_file:
        symbol = os.getenv("MT5_SYMBOL", "EURUSD")
        instrument = _make_instrument(symbol)
        engine.add_instrument(instrument)
        ticks = load_dukascopy_quote_ticks(ticks_file, instrument=instrument)
        engine.add_data(ticks)
        bar_type = BarType.from_str(f"{instrument.id}-100-TICK-MID-INTERNAL")
        return instrument, bar_type

    # Fallback: bundled sample ticks so the script runs offline out-of-the-box.
    symbol = os.getenv("MT5_SYMBOL", "AUDUSD")
    instrument = _make_instrument(symbol)
    engine.add_instrument(instrument)
    sample = TestDataProvider().read_csv_ticks("truefx/audusd-ticks.csv")
    ticks = load_dukascopy_quote_ticks(sample, instrument=instrument)
    engine.add_data(ticks)
    bar_type = BarType.from_str(f"{instrument.id}-100-TICK-MID-INTERNAL")
    return instrument, bar_type


def main() -> None:
    engine = BacktestEngine(config=BacktestEngineConfig(trader_id=TraderId("BACKTESTER-001")))

    currency = Currency.from_str(os.getenv("MT5_CURRENCY", "USD"), strict=False)
    balance = Decimal(os.getenv("MT5_BALANCE", "100000"))

    engine.add_venue(
        venue=MT5_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=currency,
        starting_balances=[Money(balance, currency)],
        fill_model=FillModel(prob_slippage=0.5, random_seed=42),
    )

    instrument, bar_type = _build_data(engine)

    risk_pct, fixed_size = parse_trade_size_spec(os.getenv("MT5_TRADE_SIZE", "0.10"))
    stop_pips = float(os.getenv("MT5_RISK_STOP_PIPS", "50"))
    if risk_pct is not None:
        print(f"Sizing: risk {risk_pct}% of equity per trade, stop {stop_pips} pips")
    else:
        print(f"Sizing: fixed {fixed_size} lots, stop {stop_pips} pips")

    strategy = MT5RiskEMACross(
        config=MT5RiskEMACrossConfig(
            instrument_id=instrument.id,
            bar_type=bar_type,
            fast_ema_period=10,
            slow_ema_period=20,
            stop_pips=stop_pips,
            risk_pct=risk_pct,
            fixed_size=fixed_size,
            account_currency=str(currency),
            tp_rr=float(os.getenv("MT5_TP_RR", "0")),
        ),
    )
    engine.add_strategy(strategy=strategy)

    engine.run()

    with pd.option_context("display.max_rows", 100, "display.max_columns", None, "display.width", 300):
        print(engine.trader.generate_account_report(MT5_VENUE))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    engine.reset()
    engine.dispose()


if __name__ == "__main__":
    main()
