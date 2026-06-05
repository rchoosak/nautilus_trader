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
Demonstrates polling Forex quotes and external bars from a MetaTrader 5 terminal.

The MetaTrader5 Python package and a logged-in MT5 terminal are required at runtime.
Credentials can be supplied through environment variables, or omitted to use the
terminal's current session.

"""

import os

from nautilus_trader.adapters.mt5 import MT5
from nautilus_trader.adapters.mt5 import MT5DataClientConfig
from nautilus_trader.adapters.mt5 import MT5InstrumentProviderConfig
from nautilus_trader.adapters.mt5 import MT5LiveDataClientFactory
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.test_kit.strategies.tester_data import DataTester
from nautilus_trader.test_kit.strategies.tester_data import DataTesterConfig


def env_int(name: str) -> int | None:
    value = os.getenv(name)
    return int(value) if value else None


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y"}


def env_csv(name: str) -> list[str] | None:
    value = os.getenv(name)
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


symbol = os.getenv("MT5_SYMBOL", "EURUSD")
instrument_id = InstrumentId.from_str(f"{symbol}.{MT5}")

config_node = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    data_clients={
        MT5: MT5DataClientConfig(
            login=env_int("MT5_LOGIN"),
            password=os.getenv("MT5_PASSWORD"),
            server=os.getenv("MT5_SERVER"),
            path=os.getenv("MT5_PATH"),
            portable=env_bool("MT5_PORTABLE"),
            instrument_provider=MT5InstrumentProviderConfig(
                load_symbols=[symbol],
                symbol_suffixes=env_csv("MT5_SYMBOL_SUFFIXES"),
            ),
            poll_interval_ms=int(os.getenv("MT5_POLL_INTERVAL_MS", "250")),
        ),
    },
    timeout_connection=30.0,
    timeout_disconnection=10.0,
    timeout_post_stop=2.0,
)

node = TradingNode(config=config_node)

tester = DataTester(
    config=DataTesterConfig(
        instrument_ids=[instrument_id],
        bar_types=[BarType.from_str(f"{instrument_id}-1-MINUTE-BID-EXTERNAL")],
        subscribe_instrument=True,
        subscribe_quotes=True,
        subscribe_bars=True,
    ),
)

node.trader.add_actor(tester)
node.add_data_client_factory(MT5, MT5LiveDataClientFactory)
node.build()

if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
