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

from types import SimpleNamespace

import pytest

from nautilus_trader.adapters.mt5.bridge import MT5TerminalBridge


class FakeMT5Module:
    def __init__(self) -> None:
        self.connected = False
        self.initialize_calls = 0
        self.login_calls = 0
        self.shutdown_calls = 0

    def initialize(self, **kwargs):
        self.connected = True
        self.initialize_calls += 1
        return True

    def login(self, **kwargs):
        self.connected = True
        self.login_calls += 1
        return True

    def shutdown(self):
        self.connected = False
        self.shutdown_calls += 1
        return True

    def terminal_info(self):
        return SimpleNamespace(connected=self.connected)

    def last_error(self):
        return (0, "OK")


@pytest.mark.asyncio
async def test_ensure_connected_does_not_reconnect_healthy_terminal() -> None:
    fake = FakeMT5Module()
    bridge = MT5TerminalBridge(login=123, password="secret")
    bridge._mt5 = fake

    await bridge.initialize()

    assert await bridge.ensure_connected(initial_delay_ms=0)
    assert fake.initialize_calls == 1
    assert fake.login_calls == 1
    assert fake.shutdown_calls == 0


@pytest.mark.asyncio
async def test_ensure_connected_reconnects_unhealthy_terminal() -> None:
    fake = FakeMT5Module()
    bridge = MT5TerminalBridge(login=123, password="secret")
    bridge._mt5 = fake

    await bridge.initialize()
    fake.connected = False

    assert await bridge.ensure_connected(max_attempts=2, initial_delay_ms=0)
    assert fake.initialize_calls == 2
    assert fake.login_calls == 2
    assert fake.shutdown_calls == 1
