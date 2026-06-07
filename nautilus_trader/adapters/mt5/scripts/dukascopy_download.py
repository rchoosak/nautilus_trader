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
Download Dukascopy historical FX data (tick or minute) and prepare it for backtesting.

The command fetches Dukascopy's public ``.bi5`` datafeed directly (LZMA-compressed binary,
no third-party dependency), decodes it, and writes either a ``ParquetDataCatalog`` (ready for
``BacktestNode``) or normalized ``parquet``/``csv`` files (read by the loaders in
``nautilus_trader.adapters.mt5.loaders``). Runs on macOS without ``MetaTrader5``.

Examples
--------
Download a day of EURUSD ticks into a catalog::

    python -m nautilus_trader.adapters.mt5.scripts.dukascopy_download \\
        -s EURUSD --start 2024-06-03 --end 2024-06-04 --granularity tick \\
        --format catalog -o ./dukascopy_catalog

Download a month of EURUSD 1-minute bars (aggregated from ticks) as parquet::

    python -m nautilus_trader.adapters.mt5.scripts.dukascopy_download \\
        -s EURUSD --start 2024-06-01 --end 2024-07-01 --granularity minute \\
        --format parquet -o ./data
"""

from __future__ import annotations

import lzma
import struct
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import click
import numpy as np
import pandas as pd

from nautilus_trader.adapters.mt5.loaders import load_dukascopy_bars
from nautilus_trader.adapters.mt5.loaders import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5.loaders import mt5_fx_instrument
from nautilus_trader.model.data import BarType


_DATAFEED = "https://datafeed.dukascopy.com/datafeed"
_USER_AGENT = "nautilus-trader-dukascopy-downloader"
_TICK_STRUCT = struct.Struct(">3i2f")  # (ms_offset, price_a, price_b, vol_a, vol_b)
_CANDLE_STRUCT = struct.Struct(">5if")  # (offset, open, close, low, high, volume)


# -------------------------------------------------------------------------------------------------
# Pure helpers (no network)
# -------------------------------------------------------------------------------------------------


def point_for_symbol(symbol: str, digits: int | None = None) -> float:
    """
    Return the price ``point`` (``10**-digits``) for an FX symbol.

    Defaults to 3 digits for ``*JPY`` pairs, otherwise 5 (matching ``mt5_fx_instrument``).
    """
    normalized = symbol.upper().replace("/", "")
    if digits is None:
        digits = 3 if normalized.endswith("JPY") else 5
    return 10.0**-digits


def dukascopy_tick_url(symbol: str, dt: datetime) -> str:
    """
    Return the Dukascopy hourly tick ``.bi5`` URL for ``symbol`` at hour ``dt``.

    Note the month component is zero-indexed (January == ``00``).
    """
    symbol = symbol.upper().replace("/", "")
    return (
        f"{_DATAFEED}/{symbol}/{dt.year:04d}/{dt.month - 1:02d}/{dt.day:02d}/"
        f"{dt.hour:02d}h_ticks.bi5"
    )


def dukascopy_candle_url(symbol: str, day: datetime, side: str = "BID") -> str:
    """
    Return the Dukascopy daily 1-minute candle ``.bi5`` URL for ``symbol`` on ``day``.

    Note the month component is zero-indexed (January == ``00``).
    """
    symbol = symbol.upper().replace("/", "")
    return (
        f"{_DATAFEED}/{symbol}/{day.year:04d}/{day.month - 1:02d}/{day.day:02d}/"
        f"{side.upper()}_candles_min_1.bi5"
    )


def _decompress(raw: bytes) -> bytes:
    if not raw:
        return b""
    try:
        return lzma.decompress(raw, format=lzma.FORMAT_AUTO)
    except lzma.LZMAError:
        return lzma.decompress(raw, format=lzma.FORMAT_ALONE)


def decode_tick_bi5(raw: bytes, *, hour_start_ms: int, point: float) -> pd.DataFrame:
    """
    Decode a Dukascopy tick ``.bi5`` payload into a normalized tick ``DataFrame``.

    Returns columns ``timestamp, bid, ask, bid_size, ask_size``. The bid/ask price columns
    are self-corrected per row so that ``bid <= ask`` (Dukascopy's field order is documented
    inconsistently across sources).
    """
    data = _decompress(raw)
    if not data:
        return _empty_tick_frame()

    records = np.frombuffer(data, dtype=">i4,>i4,>i4,>f4,>f4", count=len(data) // 20)
    offsets = records["f0"].astype("int64")
    price_a = records["f1"].astype("float64") * point
    price_b = records["f2"].astype("float64") * point
    vol_a = records["f3"].astype("float64")
    vol_b = records["f4"].astype("float64")

    # Self-correct so bid <= ask regardless of the source field order.
    bid = np.minimum(price_a, price_b)
    ask = np.maximum(price_a, price_b)
    a_is_ask = price_a >= price_b
    ask_size = np.where(a_is_ask, vol_a, vol_b)
    bid_size = np.where(a_is_ask, vol_b, vol_a)

    timestamps = pd.to_datetime(hour_start_ms + offsets, unit="ms", utc=True)
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
        },
    )


def decode_candle_bi5(raw: bytes, *, day_start_ms: int, point: float) -> pd.DataFrame:
    """
    Decode a Dukascopy 1-minute candle ``.bi5`` payload into an OHLCV ``DataFrame``.

    Returns columns ``timestamp, open, high, low, close, volume``. Rows with inconsistent
    OHLC (``low > high`` or body outside the range) are dropped.
    """
    data = _decompress(raw)
    if not data:
        return _empty_bar_frame()

    records = np.frombuffer(data, dtype=">i4,>i4,>i4,>i4,>i4,>f4", count=len(data) // 24)
    offsets = records["f0"].astype("int64")
    open_ = records["f1"].astype("float64") * point
    close = records["f2"].astype("float64") * point
    low = records["f3"].astype("float64") * point
    high = records["f4"].astype("float64") * point
    volume = records["f5"].astype("float64")

    # The offset unit is seconds for daily candle files; auto-scale to milliseconds.
    if offsets.size and offsets.max() < 100_000:
        offset_ms = offsets * 1000
    else:
        offset_ms = offsets
    timestamps = pd.to_datetime(day_start_ms + offset_ms, unit="ms", utc=True)

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        },
    )
    body_low = df[["open", "close"]].min(axis=1)
    body_high = df[["open", "close"]].max(axis=1)
    valid = (df["low"] <= body_low) & (df["high"] >= body_high) & (df["low"] <= df["high"])
    return df[valid].reset_index(drop=True)


def aggregate_ticks_to_minute(ticks: pd.DataFrame, *, price_type: str = "bid") -> pd.DataFrame:
    """
    Aggregate normalized ticks into 1-minute OHLCV bars.

    Parameters
    ----------
    ticks : pandas.DataFrame
        Tick frame with ``timestamp, bid, ask`` columns (and optional sizes).
    price_type : str, default "bid"
        The price to aggregate: ``bid``, ``ask`` or ``mid``.

    Returns
    -------
    pandas.DataFrame
        Columns ``timestamp, open, high, low, close, volume``.

    """
    if ticks.empty:
        return _empty_bar_frame()

    frame = ticks.set_index(pd.DatetimeIndex(ticks["timestamp"]))
    if price_type == "ask":
        price = frame["ask"]
    elif price_type == "mid":
        price = (frame["bid"] + frame["ask"]) / 2.0
    else:
        price = frame["bid"]

    ohlc = price.resample("1min").ohlc()
    sizes = frame.get("bid_size", 0.0) + frame.get("ask_size", 0.0)
    if isinstance(sizes, pd.Series):
        ohlc["volume"] = sizes.resample("1min").sum()
    else:
        ohlc["volume"] = price.resample("1min").count().astype(float)

    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"])
    ohlc = ohlc.reset_index().rename(columns={"index": "timestamp"})
    ohlc.columns = ["timestamp", "open", "high", "low", "close", "volume"]
    return ohlc


def _empty_tick_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "bid", "ask", "bid_size", "ask_size"])


def _empty_bar_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])


def _to_epoch_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


# -------------------------------------------------------------------------------------------------
# Network + orchestration
# -------------------------------------------------------------------------------------------------


def _http_get(url: str, *, retries: int = 3, timeout: float = 30.0) -> bytes | None:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})  # noqa: S310 (https)
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                return response.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # No data for this hour/day (market closed)
            last_error = e
        except urllib.error.URLError as e:
            last_error = e
    if last_error is not None:
        raise last_error
    return None


def _hour_range(start: datetime, end: datetime):
    current = start.replace(minute=0, second=0, microsecond=0)
    while current < end:
        yield current
        current += timedelta(hours=1)


def _day_range(start: datetime, end: datetime):
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while current < end:
        yield current
        current += timedelta(days=1)


def download_ticks(
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    workers: int = 8,
) -> pd.DataFrame:
    """
    Download and decode Dukascopy ticks for ``symbol`` over ``[start, end)``.
    """
    point = point_for_symbol(symbol)
    hours = list(_hour_range(start, end))

    def fetch(hour: datetime) -> pd.DataFrame:
        raw = _http_get(dukascopy_tick_url(symbol, hour))
        if not raw:
            return _empty_tick_frame()
        return decode_tick_bi5(raw, hour_start_ms=_to_epoch_ms(hour), point=point)

    return _concat_sorted(_map_workers(fetch, hours, workers), _empty_tick_frame())


def download_minute(
    symbol: str,
    start: datetime,
    end: datetime,
    *,
    source: str = "aggregate",
    price_type: str = "bid",
    workers: int = 8,
) -> pd.DataFrame:
    """
    Download Dukascopy 1-minute bars for ``symbol`` over ``[start, end)``.

    ``source="aggregate"`` downloads ticks and resamples; ``source="native"`` downloads
    Dukascopy's daily candle files for the ``price_type`` side.
    """
    if source == "aggregate":
        ticks = download_ticks(symbol, start, end, workers=workers)
        return aggregate_ticks_to_minute(ticks, price_type=price_type)

    point = point_for_symbol(symbol)
    side = "ASK" if price_type == "ask" else "BID"
    days = list(_day_range(start, end))

    def fetch(day: datetime) -> pd.DataFrame:
        raw = _http_get(dukascopy_candle_url(symbol, day, side))
        if not raw:
            return _empty_bar_frame()
        return decode_candle_bi5(raw, day_start_ms=_to_epoch_ms(day), point=point)

    return _concat_sorted(_map_workers(fetch, days, workers), _empty_bar_frame())


def _map_workers(fetch, items, workers: int) -> list[pd.DataFrame]:
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        return list(executor.map(fetch, items))


def _concat_sorted(frames: list[pd.DataFrame], empty: pd.DataFrame) -> pd.DataFrame:
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return empty
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("timestamp").drop_duplicates(subset="timestamp").reset_index(drop=True)
    return df


# -------------------------------------------------------------------------------------------------
# Output / prepare for backtest
# -------------------------------------------------------------------------------------------------


def write_outputs(
    df: pd.DataFrame,
    *,
    symbol: str,
    granularity: str,
    out_format: str,
    output_dir: str,
    price_type: str = "bid",
) -> str:
    """
    Write the downloaded data in the requested format and return the output path.
    """
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    normalized = symbol.upper().replace("/", "")

    if out_format in ("csv", "parquet"):
        path = output / f"{normalized}_{granularity}.{out_format}"
        if out_format == "csv":
            df.to_csv(path, index=False)
        else:
            df.to_parquet(path, index=False)
        return str(path)

    # catalog
    from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

    instrument = mt5_fx_instrument(normalized)
    catalog = ParquetDataCatalog(str(output))
    catalog.write_data([instrument])

    if granularity == "tick":
        data = load_dukascopy_quote_ticks(df, instrument=instrument)
    else:
        price_spec = {"ask": "ASK", "mid": "MID"}.get(price_type, "BID")
        bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-{price_spec}-EXTERNAL")
        data = load_dukascopy_bars(df, bar_type=bar_type, instrument=instrument)

    if data:
        catalog.write_data(data)
    return str(output)


# -------------------------------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------------------------------


@click.command()
@click.option("--symbol", "-s", "symbols", multiple=True, required=True, help="FX symbol(s), e.g. EURUSD.")
@click.option("--start", required=True, help="Inclusive start date (YYYY-MM-DD, UTC).")
@click.option("--end", required=True, help="Exclusive end date (YYYY-MM-DD, UTC).")
@click.option(
    "--granularity",
    type=click.Choice(["tick", "minute"]),
    default="tick",
    show_default=True,
)
@click.option(
    "--minute-source",
    type=click.Choice(["aggregate", "native"]),
    default="aggregate",
    show_default=True,
    help="How 1-minute bars are obtained (aggregate from ticks, or native candle files).",
)
@click.option(
    "--price-type",
    type=click.Choice(["bid", "ask", "mid"]),
    default="bid",
    show_default=True,
    help="Price used for minute bars.",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["catalog", "parquet", "csv"]),
    default="catalog",
    show_default=True,
)
@click.option("--output", "-o", default="./dukascopy_catalog", show_default=True)
@click.option("--workers", default=8, show_default=True, help="Concurrent download workers.")
def main(
    symbols: tuple[str, ...],
    start: str,
    end: str,
    granularity: str,
    minute_source: str,
    price_type: str,
    out_format: str,
    output: str,
    workers: int,
) -> None:
    """
    Download Dukascopy FX data and prepare it for backtesting.
    """
    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_dt <= start_dt:
        raise click.BadParameter("--end must be after --start")

    for symbol in symbols:
        click.echo(f"Downloading {symbol} {granularity} {start} -> {end} ...")
        if granularity == "tick":
            df = download_ticks(symbol, start_dt, end_dt, workers=workers)
        else:
            df = download_minute(
                symbol,
                start_dt,
                end_dt,
                source=minute_source,
                price_type=price_type,
                workers=workers,
            )

        if df.empty:
            click.echo(f"  No data returned for {symbol} (market closed or out of range).")
            continue

        path = write_outputs(
            df,
            symbol=symbol,
            granularity=granularity,
            out_format=out_format,
            output_dir=output,
            price_type=price_type,
        )
        span = f"{df['timestamp'].iloc[0]} .. {df['timestamp'].iloc[-1]}"
        click.echo(f"  {len(df):,} rows ({span}) -> {path}")


if __name__ == "__main__":
    main()
