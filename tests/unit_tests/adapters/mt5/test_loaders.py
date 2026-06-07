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

from decimal import Decimal

import pandas as pd
import pytest

from nautilus_trader.adapters.mt5.loaders import default_digits
from nautilus_trader.adapters.mt5.loaders import load_dukascopy_bars
from nautilus_trader.adapters.mt5.loaders import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5.loaders import mt5_fx_instrument
from nautilus_trader.adapters.mt5.loaders import parse_trade_size_spec
from nautilus_trader.adapters.mt5.loaders import risk_based_lots
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


def test_mt5_fx_instrument_rejects_non_six_letter_symbol() -> None:
    with pytest.raises(ValueError):
        mt5_fx_instrument("XAUUSDX")


def test_default_digits_fx_jpy_and_metal() -> None:
    assert default_digits("EURUSD") == 5
    assert default_digits("USDJPY") == 3
    assert default_digits("XAUUSD") == 3
    assert default_digits("XAGUSD") == 3


def test_mt5_fx_instrument_supports_gold() -> None:
    gold = mt5_fx_instrument("XAUUSD")

    assert gold.id.value == "XAUUSD.MT5"
    assert gold.price_precision == 3  # gold, not FX 5-digit
    assert gold.base_currency.code == "XAU"
    assert gold.quote_currency.code == "USD"
    assert gold.multiplier.as_double() == 100.0  # 100 oz per lot, not 100_000

    # Explicit overrides win (e.g. a broker with 2-digit gold / different contract).
    custom = mt5_fx_instrument("XAUUSD", digits=2, contract_size=1.0)
    assert custom.price_precision == 2
    assert custom.multiplier.as_double() == 1.0


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


def test_parse_trade_size_spec() -> None:
    assert parse_trade_size_spec("risk:1%") == (1.0, None)
    assert parse_trade_size_spec("risk:0.5") == (0.5, None)
    assert parse_trade_size_spec("0.10") == (None, Decimal("0.10"))


def test_risk_based_lots_eurusd() -> None:
    instrument = mt5_fx_instrument("EURUSD")
    # 1% of 100k = 1000 risk; 50 pip stop (0.005) x 100k contract = 500/lot -> 2.0 lots.
    lots = risk_based_lots(100_000.0, risk_pct=1.0, stop_pips=50.0, instrument=instrument)
    assert lots == Decimal("2.00")


def test_risk_based_lots_jpy_pip_and_min_clamp() -> None:
    usdjpy = mt5_fx_instrument("USDJPY")  # pip = 0.01
    assert risk_based_lots(100_000.0, risk_pct=1.0, stop_pips=50.0, instrument=usdjpy) == Decimal("0.02")

    eurusd = mt5_fx_instrument("EURUSD")
    # Tiny equity floors below the step and clamps up to the minimum volume.
    assert risk_based_lots(100.0, risk_pct=0.1, stop_pips=50.0, instrument=eurusd) == Decimal("0.01")
