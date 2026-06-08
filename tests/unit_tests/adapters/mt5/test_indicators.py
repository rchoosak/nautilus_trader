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

import math
from types import SimpleNamespace

import pytest

from nautilus_trader.adapters.mt5.indicators import StochasticRSI
from nautilus_trader.adapters.mt5.indicators import bollinger_percent_b
from nautilus_trader.adapters.mt5.loaders import mt5_fx_instrument
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType


def test_bollinger_percent_b() -> None:
    bb = SimpleNamespace(upper=1.2, middle=1.1, lower=1.0)
    assert bollinger_percent_b(1.0, bb) == pytest.approx(0.0)
    assert bollinger_percent_b(1.1, bb) == pytest.approx(0.5)
    assert bollinger_percent_b(1.2, bb) == pytest.approx(1.0)


def test_bollinger_percent_b_zero_width_is_safe() -> None:
    bb = SimpleNamespace(upper=1.1, middle=1.1, lower=1.1)
    assert bollinger_percent_b(1.1, bb) == pytest.approx(0.5)


def _bar_factory():
    instrument = mt5_fx_instrument("EURUSD")
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-BID-EXTERNAL")

    def make(close: float, i: int) -> Bar:
        price = instrument.make_price(close)
        return Bar(
            bar_type=bar_type,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=instrument.make_qty(1),
            ts_event=i * 60_000_000_000,
            ts_init=i * 60_000_000_000,
        )

    return make


def test_stochastic_rsi_initializes_and_bounded() -> None:
    make = _bar_factory()
    sr = StochasticRSI(rsi_period=14, stoch_period=14, k_smooth=3, d_smooth=3)

    assert not sr.initialized
    lo_k, hi_k = 100.0, 0.0
    for i in range(200):
        sr.handle_bar(make(1.10 + 0.005 * math.sin(i / 5.0), i))
        if sr.initialized:
            assert 0.0 <= sr.value_k <= 100.0
            assert 0.0 <= sr.value_d <= 100.0
            lo_k = min(lo_k, sr.value_k)
            hi_k = max(hi_k, sr.value_k)

    assert sr.initialized
    # Over an oscillating series %K should span its range (reach overbought and oversold).
    assert hi_k > 80.0
    assert lo_k < 20.0


def test_stochastic_rsi_reset() -> None:
    make = _bar_factory()
    sr = StochasticRSI()
    for i in range(60):
        sr.handle_bar(make(1.10 + 0.005 * math.sin(i / 5.0), i))
    assert sr.initialized
    sr.reset()
    assert not sr.initialized
    assert sr.value_k == 0.0
    assert sr.value_d == 0.0
