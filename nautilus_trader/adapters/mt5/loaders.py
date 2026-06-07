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
Offline data-source loaders for backtesting the MetaTrader 5 venue.

These helpers build MT5-venue instruments and load Dukascopy historical data (ticks or
bars) from local files or a pre-loaded ``pandas.DataFrame`` into Nautilus objects, without
requiring the Windows-only ``MetaTrader5`` package. The resulting instruments share the
same identifiers as the live adapter (for example ``EURUSD.MT5``), so a strategy can be
developed against a backtest and then run live unchanged.
"""

from __future__ import annotations

from decimal import Decimal
from os import PathLike

import pandas as pd

from nautilus_trader.adapters.mt5.parsing import parse_instrument
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler


# Candidate source column names (lower-cased) used to detect the timestamp column.
_TIME_CANDIDATES = ("timestamp", "gmt time", "gmttime", "time", "datetime", "date")

# Canonical target -> candidate source names (lower-cased) for Dukascopy tick data.
_TICK_SPEC = {
    "bid": ("bid", "bidprice", "bid_price"),
    "ask": ("ask", "askprice", "ask_price"),
    "bid_size": ("bidvolume", "bid_volume", "bid_size", "bidsize"),
    "ask_size": ("askvolume", "ask_volume", "ask_size", "asksize"),
}

# Canonical target -> candidate source names (lower-cased) for Dukascopy candle (bar) data.
_BAR_SPEC = {
    "open": ("open", "o"),
    "high": ("high", "h"),
    "low": ("low", "l"),
    "close": ("close", "c"),
    "volume": ("volume", "v", "vol"),
}


def mt5_fx_instrument(
    symbol: str,
    *,
    digits: int | None = None,
    contract_size: float = 100_000.0,
    volume_step: float = 0.01,
    volume_min: float = 0.01,
    volume_max: float = 100.0,
    ts_init: int | None = None,
) -> CurrencyPair:
    """
    Build an MT5-venue FX ``CurrencyPair`` for offline backtesting.

    The instrument is produced through the same ``parse_instrument`` path used by the live
    adapter (by passing a synthesised ``symbol_info`` mapping), so it is identical in shape
    to a live MT5 instrument and carries the ``<SYMBOL>.MT5`` identifier.

    Parameters
    ----------
    symbol : str
        The six-letter FX symbol, for example ``"EURUSD"`` (``"EUR/USD"`` is also accepted).
    digits : int, optional
        Price precision. Defaults to 3 for ``*JPY`` pairs, otherwise 5.
    contract_size : float, default 100_000
        The trade contract size (units per lot).
    volume_step : float, default 0.01
        The minimum volume increment, in lots.
    volume_min : float, default 0.01
        The minimum order volume, in lots.
    volume_max : float, default 100.0
        The maximum order volume, in lots.
    ts_init : int, optional
        The UNIX nanosecond initialization timestamp for the instrument.

    Returns
    -------
    CurrencyPair

    """
    normalized = symbol.upper().replace("/", "")
    if len(normalized) != 6:
        raise ValueError(f"Expected a six-letter FX symbol, was {symbol!r}")

    if digits is None:
        digits = 3 if normalized.endswith("JPY") else 5

    info = {
        "name": normalized,
        "digits": digits,
        "point": float(Decimal(10) ** -digits),
        "volume_step": volume_step,
        "volume_min": volume_min,
        "volume_max": volume_max,
        "trade_contract_size": contract_size,
        "currency_base": normalized[:3],
        "currency_profit": normalized[3:],
        "filling_mode": 2,
    }
    instrument = parse_instrument(info, ts_init=ts_init)
    assert isinstance(instrument, CurrencyPair)  # noqa: S101 (six-letter symbol is always FX)
    return instrument


def load_dukascopy_quote_ticks(
    source: str | PathLike[str] | pd.DataFrame,
    *,
    instrument: CurrencyPair,
    default_volume: float = 1_000_000.0,
    column_map: dict[str, str] | None = None,
) -> list[QuoteTick]:
    """
    Load Dukascopy tick data into Nautilus ``QuoteTick`` objects.

    Accepts a CSV/Parquet path or a pre-loaded ``DataFrame`` (such as the output of the
    ``dukascopy-python`` package or a Dukascopy web CSV export with the
    ``Gmt time, Ask, Bid, AskVolume, BidVolume`` header).

    Parameters
    ----------
    source : str, PathLike or pandas.DataFrame
        The tick data file path or a pre-loaded DataFrame.
    instrument : CurrencyPair
        The instrument the ticks belong to (see :func:`mt5_fx_instrument`).
    default_volume : float, default 1_000_000
        The synthetic size used when bid/ask volume columns are absent.
    column_map : dict[str, str], optional
        Optional explicit ``{source_column: target_column}`` overrides, where target is one
        of ``bid``, ``ask``, ``bid_size``, ``ask_size`` (for non-Dukascopy layouts).

    Returns
    -------
    list[QuoteTick]

    """
    df = _read_frame(source)
    df = _normalize_index(df)
    df = _rename_columns(df, _TICK_SPEC, column_map)

    missing = {"bid", "ask"} - set(df.columns)
    if missing:
        raise ValueError(f"Tick data missing required columns {sorted(missing)}")

    keep = [c for c in ("bid", "ask", "bid_size", "ask_size") if c in df.columns]
    wrangler = QuoteTickDataWrangler(instrument=instrument)
    return wrangler.process(df[keep], default_volume=default_volume)


def load_dukascopy_bars(
    source: str | PathLike[str] | pd.DataFrame,
    *,
    bar_type: BarType,
    instrument: CurrencyPair,
    column_map: dict[str, str] | None = None,
) -> list[Bar]:
    """
    Load Dukascopy candle data into Nautilus ``Bar`` objects.

    Accepts a CSV/Parquet path or a pre-loaded ``DataFrame`` (such as the output of the
    ``dukascopy-python`` package or a Dukascopy web CSV export with the
    ``Gmt time, Open, High, Low, Close, Volume`` header).

    Parameters
    ----------
    source : str, PathLike or pandas.DataFrame
        The candle data file path or a pre-loaded DataFrame.
    bar_type : BarType
        The bar type to assign to the produced bars (for example
        ``EURUSD.MT5-1-MINUTE-BID-EXTERNAL``).
    instrument : CurrencyPair
        The instrument the bars belong to (see :func:`mt5_fx_instrument`).
    column_map : dict[str, str], optional
        Optional explicit ``{source_column: target_column}`` overrides, where target is one
        of ``open``, ``high``, ``low``, ``close``, ``volume`` (for non-Dukascopy layouts).

    Returns
    -------
    list[Bar]

    """
    df = _read_frame(source)
    df = _normalize_index(df)
    df = _rename_columns(df, _BAR_SPEC, column_map)

    missing = {"open", "high", "low", "close"} - set(df.columns)
    if missing:
        raise ValueError(f"Bar data missing required columns {sorted(missing)}")

    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    wrangler = BarDataWrangler(bar_type=bar_type, instrument=instrument)
    return wrangler.process(df[keep])


def _read_frame(source: str | PathLike[str] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        return source.copy()
    path = str(source)
    if path.lower().endswith(".parquet"):
        return pd.read_parquet(source)
    return pd.read_csv(source)


def _normalize_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        df.index.name = "timestamp"
        return df

    lower = {str(c).strip().lower(): c for c in df.columns}
    for candidate in _TIME_CANDIDATES:
        if candidate in lower:
            col = lower[candidate]
            # Dukascopy "Gmt time" uses day-first formatting; ``dayfirst`` keeps it robust.
            df = df.set_index(pd.to_datetime(df[col], dayfirst=True, utc=False))
            df = df.drop(columns=[col])
            df.index.name = "timestamp"
            return df

    raise ValueError(
        "Could not find a timestamp column; expected a DatetimeIndex or one of "
        f"{_TIME_CANDIDATES}",
    )


def _rename_columns(
    df: pd.DataFrame,
    spec: dict[str, tuple[str, ...]],
    column_map: dict[str, str] | None,
) -> pd.DataFrame:
    rename: dict[str, str] = {}
    if column_map:
        for src, tgt in column_map.items():
            if src in df.columns:
                rename[src] = tgt

    lower = {str(c).strip().lower(): c for c in df.columns}
    taken = set(rename.values())
    for target, candidates in spec.items():
        if target in taken:
            continue
        for cand in candidates:
            src = lower.get(cand)
            if src is not None and src not in rename:
                rename[src] = target
                break

    return df.rename(columns=rename)
