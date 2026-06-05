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
Demonstrates a guarded Forex execution smoke test through a MetaTrader 5 terminal.

By default this script subscribes to data and starts the execution client without
placing orders. Set ``MT5_PLACE_ORDERS=1`` and use a demo account to enable the
test limit-order flow.

"""

import os
from decimal import Decimal

from nautilus_trader.adapters.mt5 import MT5
from nautilus_trader.adapters.mt5 import MT5DataClientConfig
from nautilus_trader.adapters.mt5 import MT5ExecClientConfig
from nautilus_trader.adapters.mt5 import MT5InstrumentProviderConfig
from nautilus_trader.adapters.mt5 import MT5LiveDataClientFactory
from nautilus_trader.adapters.mt5 import MT5LiveExecClientFactory
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.test_kit.strategies.tester_exec import ExecTester
from nautilus_trader.test_kit.strategies.tester_exec import ExecTesterConfig


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
order_qty = Decimal(os.getenv("MT5_ORDER_QTY", "0.01"))
place_orders = env_bool("MT5_PLACE_ORDERS")

instrument_provider = MT5InstrumentProviderConfig(
    load_symbols=[symbol],
    symbol_suffixes=env_csv("MT5_SYMBOL_SUFFIXES"),
)

common_client_kwargs = {
    "login": env_int("MT5_LOGIN"),
    "password": os.getenv("MT5_PASSWORD"),
    "server": os.getenv("MT5_SERVER"),
    "path": os.getenv("MT5_PATH"),
    "portable": env_bool("MT5_PORTABLE"),
    "instrument_provider": instrument_provider,
}

config_node = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    exec_engine=LiveExecEngineConfig(
        reconciliation=True,
        reconciliation_lookback_mins=1440,
        open_check_interval_secs=30.0,
        open_check_open_only=False,
        position_check_interval_secs=60.0,
    ),
    data_clients={
        MT5: MT5DataClientConfig(
            **common_client_kwargs,
            poll_interval_ms=int(os.getenv("MT5_POLL_INTERVAL_MS", "250")),
        ),
    },
    exec_clients={
        MT5: MT5ExecClientConfig(
            **common_client_kwargs,
            account_id=os.getenv("MT5_ACCOUNT_ID"),
            magic=int(os.getenv("MT5_MAGIC", "10001")),
            deviation=int(os.getenv("MT5_DEVIATION", "20")),
        ),
    },
    timeout_connection=30.0,
    timeout_reconciliation=10.0,
    timeout_portfolio=10.0,
    timeout_disconnection=10.0,
    timeout_post_stop=2.0,
)

node = TradingNode(config=config_node)

strategy = ExecTester(
    config=ExecTesterConfig(
        instrument_id=instrument_id,
        external_order_claims=[instrument_id],
        order_qty=order_qty,
        subscribe_quotes=True,
        subscribe_trades=False,
        enable_limit_buys=place_orders,
        enable_limit_sells=False,
        use_hyphens_in_client_order_ids=False,
        cancel_orders_on_stop=True,
        close_positions_on_stop=False,
        dry_run=not place_orders,
        log_data=False,
    ),
)

node.trader.add_strategy(strategy)
node.add_data_client_factory(MT5, MT5LiveDataClientFactory)
node.add_exec_client_factory(MT5, MT5LiveExecClientFactory)
node.build()

if __name__ == "__main__":
    try:
        node.run()
    finally:
        node.dispose()
