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
MetaTrader 5 integration adapter.
"""

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge
from nautilus_trader.adapters.mt5.config import MT5DataClientConfig
from nautilus_trader.adapters.mt5.config import MT5ExecClientConfig
from nautilus_trader.adapters.mt5.config import MT5InstrumentProviderConfig
from nautilus_trader.adapters.mt5.constants import MT5
from nautilus_trader.adapters.mt5.constants import MT5_CLIENT_ID
from nautilus_trader.adapters.mt5.constants import MT5_VENUE
from nautilus_trader.adapters.mt5.data import MT5DataClient
from nautilus_trader.adapters.mt5.execution import MT5ExecutionClient
from nautilus_trader.adapters.mt5.factories import MT5LiveDataClientFactory
from nautilus_trader.adapters.mt5.factories import MT5LiveExecClientFactory
from nautilus_trader.adapters.mt5.indicators import StochasticRSI
from nautilus_trader.adapters.mt5.indicators import bollinger_percent_b
from nautilus_trader.adapters.mt5.loaders import default_digits
from nautilus_trader.adapters.mt5.loaders import load_dukascopy_bars
from nautilus_trader.adapters.mt5.loaders import load_dukascopy_quote_ticks
from nautilus_trader.adapters.mt5.loaders import mt5_fx_instrument
from nautilus_trader.adapters.mt5.loaders import parse_trade_size_spec
from nautilus_trader.adapters.mt5.loaders import risk_based_lots
from nautilus_trader.adapters.mt5.loaders import risk_lots_for_stop
from nautilus_trader.adapters.mt5.providers import MT5InstrumentProvider


__all__ = [
    "MT5",
    "MT5_CLIENT_ID",
    "MT5_VENUE",
    "MT5DataClient",
    "MT5DataClientConfig",
    "MT5ExecClientConfig",
    "MT5ExecutionClient",
    "MT5InstrumentProvider",
    "MT5InstrumentProviderConfig",
    "MT5LiveDataClientFactory",
    "MT5LiveExecClientFactory",
    "MT5TerminalBridge",
    "StochasticRSI",
    "bollinger_percent_b",
    "default_digits",
    "load_dukascopy_bars",
    "load_dukascopy_quote_ticks",
    "mt5_fx_instrument",
    "parse_trade_size_spec",
    "risk_based_lots",
    "risk_lots_for_stop",
]

