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
"""

import os
from decimal import Decimal

import pandas as pd

from nautilus_trader.adapters.mt5 import MT5_VENUE
from nautilus_trader.adapters.mt5 import load_dukascopy_bars
from nautilus_trader.adapters.mt5 import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5 import mt5_fx_instrument
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.examples.strategies.ema_cross import EMACross
from nautilus_trader.examples.strategies.ema_cross import EMACrossConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestDataProvider


def _build_data(engine: BacktestEngine):
    ticks_file = os.getenv("MT5_DUKASCOPY_TICKS")
    bars_file = os.getenv("MT5_DUKASCOPY_BARS")

    if bars_file:
        symbol = os.getenv("MT5_SYMBOL", "EURUSD")
        instrument = mt5_fx_instrument(symbol)
        engine.add_instrument(instrument)
        bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-BID-EXTERNAL")
        bars = load_dukascopy_bars(bars_file, bar_type=bar_type, instrument=instrument)
        engine.add_data(bars)
        return instrument, bar_type

    if ticks_file:
        symbol = os.getenv("MT5_SYMBOL", "EURUSD")
        instrument = mt5_fx_instrument(symbol)
        engine.add_instrument(instrument)
        ticks = load_dukascopy_quote_ticks(ticks_file, instrument=instrument)
        engine.add_data(ticks)
        bar_type = BarType.from_str(f"{instrument.id}-100-TICK-MID-INTERNAL")
        return instrument, bar_type

    # Fallback: bundled sample ticks so the script runs offline out-of-the-box.
    symbol = os.getenv("MT5_SYMBOL", "AUDUSD")
    instrument = mt5_fx_instrument(symbol)
    engine.add_instrument(instrument)
    sample = TestDataProvider().read_csv_ticks("truefx/audusd-ticks.csv")
    ticks = load_dukascopy_quote_ticks(sample, instrument=instrument)
    engine.add_data(ticks)
    bar_type = BarType.from_str(f"{instrument.id}-100-TICK-MID-INTERNAL")
    return instrument, bar_type


def main() -> None:
    engine = BacktestEngine(config=BacktestEngineConfig(trader_id=TraderId("BACKTESTER-001")))

    engine.add_venue(
        venue=MT5_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        starting_balances=[Money(100_000, USD)],
        fill_model=FillModel(prob_slippage=0.5, random_seed=42),
    )

    instrument, bar_type = _build_data(engine)

    strategy = EMACross(
        config=EMACrossConfig(
            instrument_id=instrument.id,
            bar_type=bar_type,
            trade_size=Decimal("0.10"),  # lots
            fast_ema_period=10,
            slow_ema_period=20,
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
