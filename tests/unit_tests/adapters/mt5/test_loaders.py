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

import pandas as pd
import pytest

from nautilus_trader.adapters.mt5.loaders import load_dukascopy_bars
from nautilus_trader.adapters.mt5.loaders import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5.loaders import mt5_fx_instrument
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.instruments import CurrencyPair


def test_mt5_fx_instrument_builds_mt5_venue_pair() -> None:
    instrument = mt5_fx_instrument("EURUSD")

    assert isinstance(instrument, CurrencyPair)
    assert instrument.id.value == "EURUSD.MT5"
    assert instrument.price_precision == 5
    assert instrument.base_currency.code == "EUR"
    assert instrument.quote_currency.code == "USD"


def test_mt5_fx_instrument_accepts_slashed_symbol_and_jpy_precision() -> None:
    instrument = mt5_fx_instrument("USD/JPY")

    assert instrument.id.value == "USDJPY.MT5"
    assert instrument.price_precision == 3


def test_mt5_fx_instrument_rejects_non_fx_symbol() -> None:
    with pytest.raises(ValueError):
        mt5_fx_instrument("XAUUSDX")


def test_load_dukascopy_quote_ticks_from_web_csv_layout() -> None:
    instrument = mt5_fx_instrument("EURUSD")
    df = pd.DataFrame(
        {
            "Gmt time": ["01.06.2024 00:00:00.000", "01.06.2024 00:00:01.000"],
            "Ask": [1.08321, 1.08325],
            "Bid": [1.08319, 1.08322],
            "AskVolume": [1.5, 2.0],
            "BidVolume": [1.2, 1.8],
        },
    )

    ticks = load_dukascopy_quote_ticks(df, instrument=instrument)

    assert len(ticks) == 2
    assert all(isinstance(t, QuoteTick) for t in ticks)
    assert ticks[0].bid_price == instrument.make_price(1.08319)
    assert ticks[0].ask_price == instrument.make_price(1.08321)
    assert ticks[0].ts_event < ticks[1].ts_event


def test_load_dukascopy_quote_ticks_uses_default_volume_when_absent() -> None:
    instrument = mt5_fx_instrument("EURUSD")
    df = pd.DataFrame(
        {
            "timestamp": ["2024-06-01T00:00:00Z"],
            "bid": [1.08319],
            "ask": [1.08321],
        },
    )

    ticks = load_dukascopy_quote_ticks(df, instrument=instrument, default_volume=1_000_000.0)

    assert len(ticks) == 1
    assert ticks[0].bid_size == instrument.make_qty(1_000_000.0)


def test_load_dukascopy_bars_from_web_csv_layout() -> None:
    instrument = mt5_fx_instrument("EURUSD")
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-BID-EXTERNAL")
    df = pd.DataFrame(
        {
            "Gmt time": ["01.06.2024 00:00:00.000", "01.06.2024 00:01:00.000"],
            "Open": [1.0832, 1.0835],
            "High": [1.0840, 1.0838],
            "Low": [1.0830, 1.0833],
            "Close": [1.0835, 1.0834],
            "Volume": [120.0, 95.0],
        },
    )

    bars = load_dukascopy_bars(df, bar_type=bar_type, instrument=instrument)

    assert len(bars) == 2
    assert all(isinstance(b, Bar) for b in bars)
    assert bars[0].open == instrument.make_price(1.0832)
    assert bars[0].close == instrument.make_price(1.0835)


def test_load_dukascopy_bars_missing_column_raises() -> None:
    instrument = mt5_fx_instrument("EURUSD")
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-BID-EXTERNAL")
    df = pd.DataFrame({"Gmt time": ["01.06.2024 00:00:00.000"], "Open": [1.0]})

    with pytest.raises(ValueError):
        load_dukascopy_bars(df, bar_type=bar_type, instrument=instrument)
