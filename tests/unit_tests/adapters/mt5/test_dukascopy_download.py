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

import lzma
import struct
from datetime import datetime
from datetime import timezone

import pandas as pd
import pytest

from nautilus_trader.adapters.mt5.scripts.dukascopy_download import aggregate_ticks_to_minute
from nautilus_trader.adapters.mt5.scripts.dukascopy_download import decode_candle_bi5
from nautilus_trader.adapters.mt5.scripts.dukascopy_download import decode_tick_bi5
from nautilus_trader.adapters.mt5.scripts.dukascopy_download import dukascopy_candle_url
from nautilus_trader.adapters.mt5.scripts.dukascopy_download import dukascopy_tick_url
from nautilus_trader.adapters.mt5.scripts.dukascopy_download import point_for_symbol


def _epoch_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_point_for_symbol() -> None:
    assert point_for_symbol("EURUSD") == pytest.approx(1e-5)
    assert point_for_symbol("USDJPY") == pytest.approx(1e-3)
    assert point_for_symbol("EUR/JPY") == pytest.approx(1e-3)


def test_tick_url_month_is_zero_indexed() -> None:
    url = dukascopy_tick_url("EURUSD", datetime(2024, 1, 2, 3, tzinfo=timezone.utc))
    assert url.endswith("/EURUSD/2024/00/02/03h_ticks.bi5")


def test_candle_url_month_is_zero_indexed() -> None:
    url = dukascopy_candle_url("EUR/USD", datetime(2024, 12, 31, tzinfo=timezone.utc), "BID")
    assert url.endswith("/EURUSD/2024/11/31/BID_candles_min_1.bi5")


def _pack_ticks(rows: list[tuple[int, int, int, float, float]]) -> bytes:
    payload = b"".join(struct.pack(">3i2f", *row) for row in rows)
    return lzma.compress(payload)


def test_decode_tick_bi5_scales_and_timestamps() -> None:
    hour = datetime(2024, 6, 3, 0, tzinfo=timezone.utc)
    raw = _pack_ticks(
        [
            (500, 108325, 108319, 1.5, 1.2),  # (offset_ms, ask, bid, askvol, bidvol)
            (1500, 108330, 108326, 2.0, 1.8),
        ],
    )

    df = decode_tick_bi5(raw, hour_start_ms=_epoch_ms(hour), point=1e-5)

    assert len(df) == 2
    assert df["bid"].iloc[0] == pytest.approx(1.08319)
    assert df["ask"].iloc[0] == pytest.approx(1.08325)
    assert (df["bid"] <= df["ask"]).all()
    assert df["timestamp"].iloc[0] == pd.Timestamp("2024-06-03 00:00:00.500", tz="UTC")
    assert df["timestamp"].iloc[1] == pd.Timestamp("2024-06-03 00:00:01.500", tz="UTC")


def test_decode_tick_bi5_self_corrects_field_order() -> None:
    hour = datetime(2024, 6, 3, 0, tzinfo=timezone.utc)
    # Lower price first (as if bid is field A) — decoder must still yield bid <= ask.
    raw = _pack_ticks([(0, 108319, 108325, 1.2, 1.5)])

    df = decode_tick_bi5(raw, hour_start_ms=_epoch_ms(hour), point=1e-5)

    assert df["bid"].iloc[0] == pytest.approx(1.08319)
    assert df["ask"].iloc[0] == pytest.approx(1.08325)


def test_decode_tick_bi5_empty() -> None:
    df = decode_tick_bi5(b"", hour_start_ms=0, point=1e-5)
    assert df.empty
    assert list(df.columns) == ["timestamp", "bid", "ask", "bid_size", "ask_size"]


def _pack_candles(rows: list[tuple[int, int, int, int, int, float]]) -> bytes:
    payload = b"".join(struct.pack(">5if", *row) for row in rows)
    return lzma.compress(payload)


def test_decode_candle_bi5_ohlcv_and_drops_invalid() -> None:
    day = datetime(2024, 6, 3, tzinfo=timezone.utc)
    raw = _pack_candles(
        [
            (0, 108320, 108350, 108310, 108360, 120.0),  # (offset_s, open, close, low, high, vol)
            (60, 108350, 108340, 108330, 108370, 95.0),
            (120, 108300, 108300, 108400, 108200, 10.0),  # invalid: low > high -> dropped
        ],
    )

    df = decode_candle_bi5(raw, day_start_ms=_epoch_ms(day), point=1e-5)

    assert len(df) == 2
    assert df["open"].iloc[0] == pytest.approx(1.08320)
    assert df["high"].iloc[0] == pytest.approx(1.08360)
    assert df["low"].iloc[0] == pytest.approx(1.08310)
    assert df["close"].iloc[0] == pytest.approx(1.08350)
    assert df["timestamp"].iloc[0] == pd.Timestamp("2024-06-03 00:00:00", tz="UTC")
    assert df["timestamp"].iloc[1] == pd.Timestamp("2024-06-03 00:01:00", tz="UTC")


def test_aggregate_ticks_to_minute_bid_and_mid() -> None:
    ts = [
        "2024-06-03 00:00:10",
        "2024-06-03 00:00:30",
        "2024-06-03 00:01:05",
    ]
    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(ts, utc=True),
            "bid": [1.10, 1.20, 1.15],
            "ask": [1.11, 1.21, 1.16],
            "bid_size": [1.0, 1.0, 1.0],
            "ask_size": [1.0, 1.0, 1.0],
        },
    )

    bars = aggregate_ticks_to_minute(ticks, price_type="bid")

    assert len(bars) == 2
    assert bars["open"].iloc[0] == pytest.approx(1.10)
    assert bars["high"].iloc[0] == pytest.approx(1.20)
    assert bars["low"].iloc[0] == pytest.approx(1.10)
    assert bars["close"].iloc[0] == pytest.approx(1.20)
    assert bars["volume"].iloc[0] == pytest.approx(4.0)
    assert bars["close"].iloc[1] == pytest.approx(1.15)

    mids = aggregate_ticks_to_minute(ticks, price_type="mid")
    assert mids["open"].iloc[0] == pytest.approx(1.105)
