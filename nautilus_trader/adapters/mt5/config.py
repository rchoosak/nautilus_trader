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
Configuration for the MetaTrader 5 adapter.
"""

from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue


class MT5InstrumentProviderConfig(InstrumentProviderConfig, frozen=True):  # type: ignore[call-arg]
    """
    Configuration for ``MT5InstrumentProvider`` instances.

    Parameters
    ----------
    load_symbols : list[str], optional
        MT5-native symbols to load. Takes precedence over ``load_ids`` when set.
    symbol_suffixes : list[str], optional
        Broker suffixes to strip when inferring FX base/quote currencies, for example
        ``[".r", ".pro"]``.
    default_asset_class : str, default "FX"
        Asset class used when a symbol cannot be inferred as a six-letter FX pair.

    """

    load_symbols: list[str] | None = None
    symbol_suffixes: list[str] | None = None
    default_asset_class: str = "FX"

    def __eq__(self, other):
        if not isinstance(other, MT5InstrumentProviderConfig):
            return False
        return (
            self.load_all == other.load_all
            and self.load_ids == other.load_ids
            and self.filters == other.filters
            and self.load_symbols == other.load_symbols
            and self.symbol_suffixes == other.symbol_suffixes
            and self.default_asset_class == other.default_asset_class
        )

    def __hash__(self):
        filters = frozenset(self.filters.items()) if self.filters else None
        return hash(
            (
                self.load_all,
                self.load_ids,
                filters,
                tuple(self.load_symbols or ()),
                tuple(self.symbol_suffixes or ()),
                self.default_asset_class,
            ),
        )


class MT5DataClientConfig(LiveDataClientConfig, frozen=True):  # type: ignore[call-arg]
    """
    Configuration for ``MT5DataClient`` instances.

    Parameters
    ----------
    login : int, optional
        MT5 account login. If ``None`` then the terminal's current login is used.
    password : str, optional
        MT5 account password.
    server : str, optional
        MT5 broker server name.
    path : str, optional
        Path to the MT5 terminal executable.
    portable : bool, default False
        Whether to initialize the terminal in portable mode.
    timeout_ms : PositiveInt, default 60_000
        Terminal initialize/login timeout in milliseconds.
    venue : Venue, default MT5
        Nautilus venue ID.
    instrument_provider : MT5InstrumentProviderConfig, optional
        Instrument provider configuration.
    poll_interval_ms : PositiveInt, default 250
        Quote polling interval for subscriptions.
    market_depth_size : PositiveFloat, default 1_000_000
        Synthetic top-of-book size used for MT5 tick quotes, because MT5 ticks do not
        include available bid/ask quantity.

    """

    login: int | None = None
    password: str | None = None
    server: str | None = None
    path: str | None = None
    portable: bool = False
    timeout_ms: PositiveInt = 60_000
    venue: Venue = MT5_VENUE
    instrument_provider: MT5InstrumentProviderConfig = MT5InstrumentProviderConfig()
    poll_interval_ms: PositiveInt = 250
    market_depth_size: PositiveFloat = 1_000_000
    deduplicate_ticks: bool = True
    deduplicate_bars: bool = True
    emit_closed_bars: bool = True
    emit_forming_bars: bool = True
    bar_poll_lookback_multiplier: PositiveInt = 3
    warn_on_stale_ticks: bool = True
    stale_tick_threshold_ms: PositiveInt = 5_000
    health_warning_interval_ms: PositiveInt = 60_000
    reconnect_enabled: bool = True
    reconnect_initial_delay_ms: PositiveInt = 1_000
    reconnect_max_delay_ms: PositiveInt = 30_000
    reconnect_max_attempts: PositiveInt = 3
    monitor_terminal_health: bool = True
    terminal_health_check_interval_ms: PositiveInt = 30_000


class MT5ExecClientConfig(LiveExecClientConfig, frozen=True):  # type: ignore[call-arg]
    """
    Configuration for ``MT5ExecutionClient`` instances.

    Parameters
    ----------
    login : int, optional
        MT5 account login. If ``None`` then the terminal's current login is used.
    password : str, optional
        MT5 account password.
    server : str, optional
        MT5 broker server name.
    path : str, optional
        Path to the MT5 terminal executable.
    portable : bool, default False
        Whether to initialize the terminal in portable mode.
    timeout_ms : PositiveInt, default 60_000
        Terminal initialize/login timeout in milliseconds.
    venue : Venue, default MT5
        Nautilus venue ID.
    account_id : str, optional
        Account ID. If ``None`` the MT5 login is used on connect.
    oms_type : OmsType, default NETTING
        Nautilus OMS type for the MT5 account. Use HEDGING for MT5 hedging accounts.
    instrument_provider : MT5InstrumentProviderConfig, optional
        Instrument provider configuration.
    magic : int, default 10001
        MT5 magic number applied to adapter-created orders.
    deviation : int, default 20
        Max price deviation in points for market orders.
    market_depth_size : PositiveFloat, default 1_000_000
        Synthetic top-of-book size used when execution emits account/position reports.

    Notes
    -----
    Order-to-``ClientOrderId`` linkage during reconciliation relies on the MT5 order
    comment, which is capped at 31 characters. Client order IDs longer than 31 chars are
    hashed into the comment and cannot be linked back to the order after a node restart.
    Keep client order IDs within 31 chars (for example with
    ``use_hyphens_in_client_order_ids=False`` and short trader/strategy IDs) to preserve
    reconciliation linkage.

    """

    login: int | None = None
    password: str | None = None
    server: str | None = None
    path: str | None = None
    portable: bool = False
    timeout_ms: PositiveInt = 60_000
    venue: Venue = MT5_VENUE
    account_id: str | None = None
    oms_type: OmsType = OmsType.NETTING
    instrument_provider: MT5InstrumentProviderConfig = MT5InstrumentProviderConfig()
    magic: int = 10001
    deviation: int = 20
    market_depth_size: PositiveFloat = 1_000_000
    use_order_check: bool = True
    filter_position_reports_by_magic: bool = True
    persist_reconciliation_mappings: bool = True
    reconciliation_lookback_days: PositiveInt = 30
    reconnect_enabled: bool = True
    reconnect_initial_delay_ms: PositiveInt = 1_000
    reconnect_max_delay_ms: PositiveInt = 30_000
    reconnect_max_attempts: PositiveInt = 3
    monitor_terminal_health: bool = True
    terminal_health_check_interval_ms: PositiveInt = 30_000
