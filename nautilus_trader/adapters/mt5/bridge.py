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
Thread-safe async bridge over the synchronous MetaTrader5 Python package.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Lock
from typing import Any


class MT5TerminalBridge:
    """
    Wraps the synchronous ``MetaTrader5`` Python module behind async methods.
    """

    def __init__(
        self,
        *,
        login: int | None = None,
        password: str | None = None,
        server: str | None = None,
        path: str | None = None,
        portable: bool = False,
        timeout_ms: int = 60_000,
    ) -> None:
        self.login = login
        self.password = password
        self.server = server
        self.path = path
        self.portable = portable
        self.timeout_ms = timeout_ms

        self._mt5: Any | None = None
        self._lock = Lock()
        self._connected = False
        self._ref_count = 0
        self._init_lock = asyncio.Lock()
        self._executor: ThreadPoolExecutor | None = None

    @property
    def mt5(self) -> Any:
        if self._mt5 is None:
            try:
                import MetaTrader5 as mt5
            except ImportError as e:
                raise RuntimeError(
                    "The MetaTrader5 Python package is required for the MT5 adapter. "
                    "Install it in the environment running the TradingNode.",
                ) from e

            self._mt5 = mt5

        return self._mt5

    @property
    def connected(self) -> bool:
        return self._connected

    def _get_executor(self) -> ThreadPoolExecutor:
        # MT5 calls run on a dedicated single-worker executor so that the MetaTrader5
        # module's blocking IPC does not occupy Nautilus's shared default thread pool.
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mt5-bridge")
        return self._executor

    async def run(self, fn_name: str, *args: Any, **kwargs: Any) -> Any:
        """
        Run an MT5 module function on the dedicated executor.
        """
        # The MetaTrader5 module is not thread-safe and backs a single terminal
        # connection, so every call is serialized under ``_lock``. This is an inherent,
        # unavoidable tradeoff: order submission can queue behind in-flight quote polling.
        def call() -> Any:
            with self._lock:
                fn = getattr(self.mt5, fn_name)
                return fn(*args, **kwargs)

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._get_executor(), call)

    async def initialize(self) -> None:
        # The bridge is shared between the data and execution clients (see the cached
        # factory), so connections are reference counted and the terminal is only
        # initialized once.
        async with self._init_lock:
            if self._connected:
                self._ref_count += 1
                return

            kwargs: dict[str, Any] = {
                "timeout": self.timeout_ms,
                "portable": self.portable,
            }
            if self.path is not None:
                kwargs["path"] = self.path
            if self.login is not None:
                kwargs["login"] = self.login
            if self.password is not None:
                kwargs["password"] = self.password
            if self.server is not None:
                kwargs["server"] = self.server

            initialized = await self.run("initialize", **kwargs)
            if not initialized:
                raise RuntimeError(f"MT5 initialize failed: {self.last_error()}")

            if self.login is not None and self.password is not None:
                login_kwargs: dict[str, Any] = {
                    "login": self.login,
                    "password": self.password,
                    "timeout": self.timeout_ms,
                }
                if self.server is not None:
                    login_kwargs["server"] = self.server

                logged_in = await self.run("login", **login_kwargs)
                if not logged_in:
                    raise RuntimeError(f"MT5 login failed: {self.last_error()}")

            self._connected = True
            self._ref_count += 1

    async def shutdown(self) -> None:
        # Only shut the terminal down once the last client has disconnected, otherwise
        # a client sharing the bridge would lose its connection prematurely.
        async with self._init_lock:
            if self._ref_count > 0:
                self._ref_count -= 1
            if self._ref_count > 0:
                return
            if self._mt5 is None or not self._connected:
                return

            await self.run("shutdown")
            self._connected = False

            if self._executor is not None:
                self._executor.shutdown(wait=False)
                self._executor = None

    def last_error(self) -> Any:
        if self._mt5 is None:
            return None
        with self._lock:
            return self.mt5.last_error()

    async def account_info(self) -> Any:
        return await self.run("account_info")

    async def terminal_info(self) -> Any:
        return await self.run("terminal_info")

    async def symbols_get(self, group: str | None = None) -> Any:
        if group is not None:
            return await self.run("symbols_get", group=group)
        return await self.run("symbols_get")

    async def symbol_info(self, symbol: str) -> Any:
        return await self.run("symbol_info", symbol)

    async def symbol_select(self, symbol: str, enable: bool = True) -> Any:
        return await self.run("symbol_select", symbol, enable)

    async def symbol_info_tick(self, symbol: str) -> Any:
        return await self.run("symbol_info_tick", symbol)

    async def copy_ticks_range(self, symbol: str, start: datetime, end: datetime, flags: int) -> Any:
        return await self.run("copy_ticks_range", symbol, start, end, flags)

    async def copy_rates_range(self, symbol: str, timeframe: int, start: datetime, end: datetime) -> Any:
        return await self.run("copy_rates_range", symbol, timeframe, start, end)

    async def order_check(self, request: dict[str, Any]) -> Any:
        return await self.run("order_check", request)

    async def order_send(self, request: dict[str, Any]) -> Any:
        return await self.run("order_send", request)

    async def orders_get(self, **kwargs: Any) -> Any:
        return await self.run("orders_get", **kwargs)

    async def positions_get(self, **kwargs: Any) -> Any:
        return await self.run("positions_get", **kwargs)

    async def history_orders_get(self, start: datetime, end: datetime, **kwargs: Any) -> Any:
        return await self.run("history_orders_get", start, end, **kwargs)

    async def history_deals_get(self, start: datetime, end: datetime, **kwargs: Any) -> Any:
        return await self.run("history_deals_get", start, end, **kwargs)
