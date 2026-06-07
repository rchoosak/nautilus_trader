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
Factories for the MetaTrader 5 adapter.
"""

import asyncio
from functools import lru_cache

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge
from nautilus_trader.adapters.mt5.config import MT5DataClientConfig
from nautilus_trader.adapters.mt5.config import MT5ExecClientConfig
from nautilus_trader.adapters.mt5.config import MT5InstrumentProviderConfig
from nautilus_trader.adapters.mt5.data import MT5DataClient
from nautilus_trader.adapters.mt5.execution import MT5ExecutionClient
from nautilus_trader.adapters.mt5.providers import MT5InstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@lru_cache(maxsize=1)
def get_cached_mt5_bridge(
    login: int | None = None,
    password: str | None = None,
    server: str | None = None,
    path: str | None = None,
    portable: bool = False,
    timeout_ms: int = 60_000,
) -> MT5TerminalBridge:
    """
    Cache and return an MT5 terminal bridge.
    """
    return MT5TerminalBridge(
        login=login,
        password=password,
        server=server,
        path=path,
        portable=portable,
        timeout_ms=timeout_ms,
    )


@lru_cache(maxsize=1)
def get_cached_mt5_instrument_provider(
    bridge: MT5TerminalBridge,
    config: MT5InstrumentProviderConfig,
) -> MT5InstrumentProvider:
    """
    Cache and return an MT5 instrument provider.
    """
    return MT5InstrumentProvider(
        bridge=bridge,
        config=config,
    )


class MT5LiveDataClientFactory(LiveDataClientFactory):
    """
    Provides an MT5 live data client factory.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str | None,
        config: MT5DataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> MT5DataClient:
        bridge = get_cached_mt5_bridge(
            login=config.login,
            password=config.password,
            server=config.server,
            path=config.path,
            portable=config.portable,
            timeout_ms=config.timeout_ms,
        )
        provider = get_cached_mt5_instrument_provider(
            bridge=bridge,
            config=config.instrument_provider,
        )
        return MT5DataClient(
            loop=loop,
            bridge=bridge,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class MT5LiveExecClientFactory(LiveExecClientFactory):
    """
    Provides an MT5 live execution client factory.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str | None,
        config: MT5ExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> MT5ExecutionClient:
        bridge = get_cached_mt5_bridge(
            login=config.login,
            password=config.password,
            server=config.server,
            path=config.path,
            portable=config.portable,
            timeout_ms=config.timeout_ms,
        )
        provider = get_cached_mt5_instrument_provider(
            bridge=bridge,
            config=config.instrument_provider,
        )
        return MT5ExecutionClient(
            loop=loop,
            bridge=bridge,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
