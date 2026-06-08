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
Offline MT5 multi-timeframe (M5 trend + M1 entry) backtest from Dukascopy tick data.

Strategy rules (mechanical implementation of a user-supplied playbook; NOT financial advice):
  * M5 trend gate: close > EMA9, EMA9 > EMA26 (diverging), RSI14 > 50 and rising, %B > 0.5.
  * M1 trigger: Stochastic RSI was oversold (<20) then %K crosses %D up through 20
    (optional confirm: M1 EMA9>EMA26 and %B > 0.5). SELL mirrors.
  * SL: swing low/high of recent M1 bars +/- buffer; size risks MT5_TRADE_SIZE of equity.
  * Manage: scale out 50% at first target (%B 1/0 or Stoch extreme) + move SL to breakeven;
    ride the rest until M5 trend reverses (EMA9/EMA26 cross) or M5 closes beyond EMA26.
  * Filters: skip sideways (tight EMAs / RSI 45-55) and trade only inside session windows.

Environment variables (all optional):
  MT5_DUKASCOPY_TICKS=/path/ticks.parquet   data (else bundled sample); MT5_SYMBOL, MT5_BALANCE,
  MT5_CURRENCY, MT5_LEVERAGE, MT5_PIP_SIZE, MT5_TRADE_SIZE=risk:1%, MT5_DIGITS, MT5_CONTRACT_SIZE,
  MT5_M1_SPEC (default 1-MINUTE-BID-INTERNAL), MT5_M5_SPEC (default 5-MINUTE-BID-INTERNAL),
  MT5_SESSIONS (default "07:00-10:00,12:00-15:30" UTC = Thai 14:00-17:00 / 19:00-22:30),
  MT5_SWING_LOOKBACK, MT5_STOP_BUFFER_PIPS, MT5_MIN_EMA_GAP_PIPS, MT5_M1_CONFIRM.

Note: news (Nonfarm/CPI) blackout needs an economic-calendar feed and is omitted in backtest.
"""

import os
from decimal import Decimal

import pandas as pd

from nautilus_trader.adapters.mt5 import MT5_VENUE
from nautilus_trader.adapters.mt5 import StochasticRSI
from nautilus_trader.adapters.mt5 import bollinger_percent_b
from nautilus_trader.adapters.mt5 import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5 import mt5_fx_instrument
from nautilus_trader.adapters.mt5 import parse_trade_size_spec
from nautilus_trader.adapters.mt5 import risk_lots_for_stop
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.datetime import unix_nanos_to_dt
from nautilus_trader.indicators import BollingerBands
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.indicators import RelativeStrengthIndex
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.position import Position
from nautilus_trader.test_kit.providers import TestDataProvider
from nautilus_trader.trading.strategy import Strategy


def _parse_sessions(spec: str) -> list[tuple[int, int]]:
    windows = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        start, end = part.split("-")
        sh, sm = (int(x) for x in start.split(":"))
        eh, em = (int(x) for x in end.split(":"))
        windows.append((sh * 60 + sm, eh * 60 + em))
    return windows


class MT5MtfConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``MT5MtfStrategy``.
    """

    instrument_id: InstrumentId
    m1_bar_type: BarType
    m5_bar_type: BarType
    ema_fast: int = 9
    ema_slow: int = 26
    rsi_period: int = 14
    bb_period: int = 20
    bb_k: float = 2.0
    stoch_period: int = 14
    risk_pct: float = 1.0
    account_currency: str = "USD"
    pip_size: float | None = None
    swing_lookback: int = 8
    stop_buffer_pips: float = 12.0
    min_ema_gap_pips: float = 5.0
    sessions: str = "07:00-10:00,12:00-15:30"
    require_m1_confirm: bool = True


class MT5MtfStrategy(Strategy):
    """
    Multi-timeframe EMA/RSI/Bollinger + Stochastic-RSI strategy (see module docstring).
    """

    def __init__(self, config: MT5MtfConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None

        self.ema_fast_m5 = ExponentialMovingAverage(config.ema_fast)
        self.ema_slow_m5 = ExponentialMovingAverage(config.ema_slow)
        self.rsi_m5 = RelativeStrengthIndex(config.rsi_period)
        self.bb_m5 = BollingerBands(config.bb_period, config.bb_k)

        self.ema_fast_m1 = ExponentialMovingAverage(config.ema_fast)
        self.ema_slow_m1 = ExponentialMovingAverage(config.ema_slow)
        self.bb_m1 = BollingerBands(config.bb_period, config.bb_k)
        self.stoch_rsi = StochasticRSI(config.rsi_period, config.stoch_period)

        self._pip = 0.0
        self._sessions = _parse_sessions(config.sessions)
        self._swing: list[tuple[float, float]] = []

        # Per-M5 state
        self._m5_close = 0.0
        self._prev_gap_m5 = 0.0
        self._prev_rsi_m5 = 0.0
        self._m5_seen = False
        # Per-M1 trigger state
        self._prev_k = 50.0
        self._prev_d = 50.0
        # Per-trade state
        self._entry_pending = False
        self._pending_sl = 0.0
        self._pending_side = OrderSide.BUY
        self._entry_px = 0.0
        self._sl_order_id = None
        self._tp1_done = False

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"No instrument {self.config.instrument_id}")
            self.stop()
            return

        self._pip = self.config.pip_size or float(self.instrument.price_increment) * 10.0

        for ind in (self.ema_fast_m5, self.ema_slow_m5, self.rsi_m5, self.bb_m5):
            self.register_indicator_for_bars(self.config.m5_bar_type, ind)
        for ind in (self.ema_fast_m1, self.ema_slow_m1, self.bb_m1):
            self.register_indicator_for_bars(self.config.m1_bar_type, ind)

        self.subscribe_bars(self.config.m5_bar_type)
        self.subscribe_bars(self.config.m1_bar_type)

    # -- Bar dispatch -----------------------------------------------------------------------------

    def on_bar(self, bar: Bar) -> None:
        if bar.bar_type == self.config.m5_bar_type:
            self._on_m5_bar(bar)
        elif bar.bar_type == self.config.m1_bar_type:
            self._on_m1_bar(bar)

    def _on_m5_bar(self, bar: Bar) -> None:
        if not self.bb_m5.initialized:
            return

        self._m5_close = float(bar.close)
        gap = self.ema_fast_m5.value - self.ema_slow_m5.value
        self._m5_diverging = self._m5_seen and abs(gap) >= self._min_gap() and abs(gap) > abs(self._prev_gap_m5)
        self._m5_rsi_rising = self._m5_seen and self.rsi_m5.value > self._prev_rsi_m5
        self._prev_gap_m5 = gap
        self._prev_rsi_m5 = self.rsi_m5.value
        self._m5_seen = True

        # Trend-based management of an open position.
        position = self._open_position()
        if position is None:
            return
        long = position.side == PositionSide.LONG
        reverse = (long and self.ema_fast_m5.value < self.ema_slow_m5.value) or (
            not long and self.ema_fast_m5.value > self.ema_slow_m5.value
        )
        manual_cut = (long and self._m5_close < self.ema_slow_m5.value) or (
            not long and self._m5_close > self.ema_slow_m5.value
        )
        if reverse or manual_cut:
            self._exit_all("m5 reverse/cut")

    def _on_m1_bar(self, bar: Bar) -> None:
        self.stoch_rsi.handle_bar(bar)
        self._swing.append((float(bar.low), float(bar.high)))
        if len(self._swing) > self.config.swing_lookback:
            self._swing.pop(0)

        k, d = self.stoch_rsi.value_k, self.stoch_rsi.value_d
        if self._ready():
            if not self.portfolio.is_flat(self.config.instrument_id):
                self._manage_open(bar)
            elif not self._entry_pending:
                self._reset_trade()
                if self._in_session(bar.ts_event):
                    side = self._entry_signal(bar, k, d)
                    if side is not None:
                        self._enter(side, bar)

        self._prev_k, self._prev_d = k, d

    # -- Signal -----------------------------------------------------------------------------------

    def _entry_signal(self, bar: Bar, k: float, d: float) -> OrderSide | None:
        gap = self.ema_fast_m5.value - self.ema_slow_m5.value
        if abs(gap) < self._min_gap() or 45.0 <= self.rsi_m5.value <= 55.0:
            return None  # sideways guard

        pb_m5 = bollinger_percent_b(self._m5_close, self.bb_m5)
        long_trend = (
            self._m5_close > self.ema_fast_m5.value
            and self.ema_fast_m5.value > self.ema_slow_m5.value
            and self._m5_diverging
            and self.rsi_m5.value > 50.0
            and self._m5_rsi_rising
            and pb_m5 > 0.5
        )
        short_trend = (
            self._m5_close < self.ema_fast_m5.value
            and self.ema_fast_m5.value < self.ema_slow_m5.value
            and self._m5_diverging
            and self.rsi_m5.value < 50.0
            and not self._m5_rsi_rising
            and pb_m5 < 0.5
        )

        pb_m1 = bollinger_percent_b(float(bar.close), self.bb_m1)
        m1_up = self.ema_fast_m1.value > self.ema_slow_m1.value
        confirm_long = (not self.config.require_m1_confirm) or (m1_up and pb_m1 > 0.5)
        confirm_short = (not self.config.require_m1_confirm) or (not m1_up and pb_m1 < 0.5)

        # Stochastic RSI: cross through the 20/80 levels out of oversold/overbought.
        trig_long = self._prev_k < 20.0 and self._prev_k <= self._prev_d and k > d
        trig_short = self._prev_k > 80.0 and self._prev_k >= self._prev_d and k < d

        if long_trend and trig_long and confirm_long:
            return OrderSide.BUY
        if short_trend and trig_short and confirm_short:
            return OrderSide.SELL
        return None

    # -- Orders -----------------------------------------------------------------------------------

    def _enter(self, side: OrderSide, bar: Bar) -> None:
        if self.instrument is None or not self._swing:
            return
        buffer_ = self.config.stop_buffer_pips * self._pip
        entry_ref = float(bar.close)
        if side == OrderSide.BUY:
            sl_price = min(low for low, _ in self._swing) - buffer_
        else:
            sl_price = max(high for _, high in self._swing) + buffer_

        stop_distance = abs(entry_ref - sl_price)
        if stop_distance <= 0:
            return

        equity = self._equity()
        if equity is None:
            return
        qty = self.instrument.make_qty(
            risk_lots_for_stop(
                equity,
                risk_pct=self.config.risk_pct,
                stop_distance=stop_distance,
                instrument=self.instrument,
            ),
        )

        self._pending_sl = sl_price
        self._pending_side = side
        self._entry_pending = True
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=qty,
        )
        self.submit_order(order)

    def on_position_opened(self, event) -> None:
        position = self.cache.position(event.position_id)
        if position is None or self.instrument is None:
            return
        self._entry_pending = False
        self._tp1_done = False
        self._entry_px = float(position.avg_px_open)

        sl_side = OrderSide.SELL if position.side == PositionSide.LONG else OrderSide.BUY
        sl = self.order_factory.stop_market(
            instrument_id=self.config.instrument_id,
            order_side=sl_side,
            quantity=position.quantity,
            trigger_price=self.instrument.make_price(self._pending_sl),
            reduce_only=True,
        )
        self._sl_order_id = sl.client_order_id
        self.submit_order(sl)

    def _manage_open(self, bar: Bar) -> None:
        if self._tp1_done or self.instrument is None:
            return
        position = self._open_position()
        if position is None:
            return

        long = position.side == PositionSide.LONG
        pb = bollinger_percent_b(float(bar.close), self.bb_m1)
        k = self.stoch_rsi.value_k
        hit = (long and (pb >= 1.0 or k >= 80.0)) or (not long and (pb <= 0.0 or k <= 20.0))
        if not hit:
            return

        self._tp1_done = True
        half = self.instrument.make_qty(float(position.quantity) * 0.5)
        if self.instrument.min_quantity is not None and half < self.instrument.min_quantity:
            return  # too small to scale out; let it ride with the stop

        reduce_side = OrderSide.SELL if long else OrderSide.BUY
        self.submit_order(
            self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=reduce_side,
                quantity=half,
                reduce_only=True,
            ),
        )
        sl_order = self.cache.order(self._sl_order_id) if self._sl_order_id else None
        if sl_order is not None and not sl_order.is_closed:
            self.modify_order(sl_order, trigger_price=self.instrument.make_price(self._entry_px))

    def _exit_all(self, reason: str) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)
        self._reset_trade()

    # -- Helpers ----------------------------------------------------------------------------------

    def _ready(self) -> bool:
        return (
            self.bb_m5.initialized
            and self.rsi_m5.initialized
            and self.ema_slow_m5.initialized
            and self.bb_m1.initialized
            and self.ema_slow_m1.initialized
            and self.stoch_rsi.initialized
            and self._m5_seen
        )

    def _min_gap(self) -> float:
        return self.config.min_ema_gap_pips * self._pip

    def _in_session(self, ts_event: int) -> bool:
        if not self._sessions:
            return True
        dt = unix_nanos_to_dt(ts_event)
        minutes = dt.hour * 60 + dt.minute
        return any(start <= minutes < end for start, end in self._sessions)

    def _open_position(self) -> Position | None:
        positions = self.cache.positions_open(instrument_id=self.config.instrument_id)
        return positions[0] if positions else None

    def _equity(self) -> float | None:
        account = self.portfolio.account(self.config.instrument_id.venue)
        if account is None:
            return None
        money = account.balance_total(Currency.from_str(self.config.account_currency, strict=False))
        return None if money is None else money.as_double()

    def _reset_trade(self) -> None:
        self._sl_order_id = None
        self._tp1_done = False
        self._entry_pending = False

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)
        self.unsubscribe_bars(self.config.m1_bar_type)
        self.unsubscribe_bars(self.config.m5_bar_type)

    def on_reset(self) -> None:
        for ind in (
            self.ema_fast_m5,
            self.ema_slow_m5,
            self.rsi_m5,
            self.bb_m5,
            self.ema_fast_m1,
            self.ema_slow_m1,
            self.bb_m1,
            self.stoch_rsi,
        ):
            ind.reset()


# -------------------------------------------------------------------------------------------------
# Runner
# -------------------------------------------------------------------------------------------------


def _make_instrument(symbol: str):
    digits = os.getenv("MT5_DIGITS")
    contract_size = os.getenv("MT5_CONTRACT_SIZE")
    return mt5_fx_instrument(
        symbol,
        digits=int(digits) if digits else None,
        contract_size=float(contract_size) if contract_size else None,
    )


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
        default_leverage=Decimal(os.getenv("MT5_LEVERAGE", "100")),
        fill_model=FillModel(prob_slippage=0.5, random_seed=42),
    )

    ticks_file = os.getenv("MT5_DUKASCOPY_TICKS")
    if ticks_file:
        symbol = os.getenv("MT5_SYMBOL", "EURUSD")
        instrument = _make_instrument(symbol)
        ticks = load_dukascopy_quote_ticks(ticks_file, instrument=instrument)
    else:
        symbol = os.getenv("MT5_SYMBOL", "AUDUSD")
        instrument = _make_instrument(symbol)
        sample = TestDataProvider().read_csv_ticks("truefx/audusd-ticks.csv")
        ticks = load_dukascopy_quote_ticks(sample, instrument=instrument)

    engine.add_instrument(instrument)
    engine.add_data(ticks)

    m1_spec = os.getenv("MT5_M1_SPEC", "1-MINUTE-BID-INTERNAL")
    m5_spec = os.getenv("MT5_M5_SPEC", "5-MINUTE-BID-INTERNAL")
    pip_env = os.getenv("MT5_PIP_SIZE")
    risk_pct, _ = parse_trade_size_spec(os.getenv("MT5_TRADE_SIZE", "risk:1%"))

    strategy = MT5MtfStrategy(
        config=MT5MtfConfig(
            instrument_id=instrument.id,
            m1_bar_type=BarType.from_str(f"{instrument.id}-{m1_spec}"),
            m5_bar_type=BarType.from_str(f"{instrument.id}-{m5_spec}"),
            risk_pct=risk_pct if risk_pct is not None else 1.0,
            account_currency=str(currency),
            pip_size=float(pip_env) if pip_env else None,
            swing_lookback=int(os.getenv("MT5_SWING_LOOKBACK", "8")),
            stop_buffer_pips=float(os.getenv("MT5_STOP_BUFFER_PIPS", "12")),
            min_ema_gap_pips=float(os.getenv("MT5_MIN_EMA_GAP_PIPS", "5")),
            sessions=os.getenv("MT5_SESSIONS", "07:00-10:00,12:00-15:30"),
            require_m1_confirm=os.getenv("MT5_M1_CONFIRM", "1") not in {"0", "false", "False"},
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
