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
Helper indicators for MetaTrader 5 strategies that are not built into Nautilus.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from nautilus_trader.indicators import RelativeStrengthIndex
from nautilus_trader.model.data import Bar


class StochasticRSI:
    """
    Stochastic RSI oscillator (``%K`` / ``%D`` in the range 0 to 100).

    Nautilus has no built-in Stochastic RSI, so this wraps a ``RelativeStrengthIndex`` and
    applies a stochastic transform with ``%K``/``%D`` smoothing. It is a plain helper (not a
    Cython ``Indicator``); call :meth:`handle_bar` manually from the strategy.

    Parameters
    ----------
    rsi_period : int, default 14
        The RSI lookback period.
    stoch_period : int, default 14
        The stochastic lookback over the RSI series.
    k_smooth : int, default 3
        The smoothing period for ``%K``.
    d_smooth : int, default 3
        The smoothing period for ``%D``.

    """

    def __init__(
        self,
        rsi_period: int = 14,
        stoch_period: int = 14,
        k_smooth: int = 3,
        d_smooth: int = 3,
    ) -> None:
        self._rsi = RelativeStrengthIndex(rsi_period)
        self._rsi_window: deque[float] = deque(maxlen=stoch_period)
        self._k_window: deque[float] = deque(maxlen=k_smooth)
        self._d_window: deque[float] = deque(maxlen=d_smooth)
        self._stoch_period = stoch_period
        self.value_k = 0.0
        self.value_d = 0.0
        self.initialized = False

    def handle_bar(self, bar: Bar) -> None:
        self._rsi.handle_bar(bar)
        if not self._rsi.initialized:
            return

        self._rsi_window.append(self._rsi.value)
        low = min(self._rsi_window)
        high = max(self._rsi_window)
        raw = 0.0 if high <= low else (self._rsi.value - low) / (high - low) * 100.0

        self._k_window.append(raw)
        self.value_k = sum(self._k_window) / len(self._k_window)
        self._d_window.append(self.value_k)
        self.value_d = sum(self._d_window) / len(self._d_window)

        if len(self._rsi_window) >= self._stoch_period:
            self.initialized = True

    def reset(self) -> None:
        self._rsi.reset()
        self._rsi_window.clear()
        self._k_window.clear()
        self._d_window.clear()
        self.value_k = 0.0
        self.value_d = 0.0
        self.initialized = False


def bollinger_percent_b(price: float, bb: Any) -> float:
    """
    Return Bollinger %B for ``price`` given a ``BollingerBands`` indicator.

    ``%B = (price - lower) / (upper - lower)``; returns 0.5 when the band width is zero.
    """
    width = bb.upper - bb.lower
    if width <= 0:
        return 0.5
    return (price - bb.lower) / width
