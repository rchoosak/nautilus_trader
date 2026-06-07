# MetaTrader 5 Forex Integration Plan

This plan tracks the work required for NautilusTrader to trade Forex through a
MetaTrader 5 terminal. The implementation target is a Python live adapter that
uses the `MetaTrader5` package as the terminal bridge.

## Current Implementation Scope

Status: in progress.

The first implementation slice focuses on:

- MT5 adapter package structure.
- Config classes for data, execution, and instrument loading.
- Async bridge over the synchronous `MetaTrader5` Python package.
- MT5 symbol parsing into Nautilus instruments.
- Quote/bar polling for live market data.
- Tick/bar deduplication and explicit closed/forming bar polling controls.
- Basic order submission/cancel/modify execution flow.
- Reconciliation reports for orders, fills, and positions.
- Symbol rule validation and `order_check` before `order_send`.
- Account state mapping for MT5 equity, margin, free margin, and account-wide
  margin reports.
- Netting/hedging-aware MT5 position report identity.
- Configurable inclusion of manual/external MT5 positions in position
  reconciliation.
- Smoke-test examples for data and guarded execution.

## Missing Features And Work Plan

### Phase 1 - Safety And Broker Rules

Status: started.

- Add symbol rule parsing for `volume_min`, `volume_max`, `volume_step`,
  `trade_stops_level`, `trade_freeze_level`, `filling_mode`, `order_mode`, and
  `trade_mode`.
- Validate volume min/max/step before sending orders.
- Validate market is trade-enabled before sending orders.
- Call MT5 `order_check` before `order_send`.
- Support broker-specific filling modes instead of one fixed policy.
- Normalize price/volume through the loaded Nautilus instrument.

### Phase 2 - Execution Coverage

Status: started.

- Add robust retcode mapping for all common MT5 trade return codes. Started:
  adapter errors now include the symbolic MT5 retcode name when available.
- Support partial close and MT5-specific close-by operations. Started:
  market orders can carry an MT5 `position` from `position_id` or
  `mt5_position_id`; close-by can be requested with `mt5_position_by`.
- Improve pending order modification for stop-limit and stop-market variants.
  Started: modify requests now map limit, stop-market, and stop-limit price
  fields separately and reject pending order volume modification.
- Add native handling for order expiration modes where broker supports them.
  Started: submit requests map `DAY` and `GTD`; modify requests can set
  `expire_time_ns` through params.
- Add optional support for bracket/OCO-style behavior through Nautilus order
  management, with clear constraints when MT5 has no native equivalent.
- Add trailing stop support where broker/terminal behavior can be verified.

### Phase 3 - Account And Position Accuracy

Status: started.

- Separate behavior for netting and hedging accounts. Started: position status
  reports now use the stable MT5 `identifier` for netting accounts and the
  per-position `ticket` for hedging accounts.
- Improve position report identity for hedging accounts. Started: hedging mode
  preserves each MT5 position ticket as the venue position ID.
- Map balance, equity, margin, free margin, margin level, credit, realized PnL,
  swap, and commission fields more completely. Started: account state now maps
  MT5 `equity`, `margin`, and free margin into Nautilus `AccountBalance`, and
  stores MT5 `balance`, `credit`, `profit`, `margin_free`, `margin_level`, stop
  out fields, assets/liabilities, and blocked commission in `AccountState.info`.
- Add margin reports where the Nautilus model can represent them. Started:
  account-wide MT5 margin is emitted as `MarginBalance` with initial and
  maintenance values set to the reported used margin until per-instrument margin
  can be reliably inferred.
- Decide how to treat manual/external MT5 orders and positions on the same
  account. Started: order/fill reports remain scoped to the adapter `magic`
  number, while position reports can include manual/external MT5 positions by
  setting `filter_position_reports_by_magic=False`.
- Remaining: map realized PnL, swap, and commission into reports where Nautilus
  exposes matching fields or persist them in reconciliation metadata.
- Remaining: add fake-bridge tests covering account state, hedging ticket
  identity, netting identifier identity, and external-position inclusion.
- Remaining: verify account values against demo-account terminal statements for
  brokers using different margin modes and stop-out modes.

### Phase 4 - Reconciliation Durability

Status: started.

- Persist the mapping between Nautilus `ClientOrderId`, MT5 order ticket, deal
  ticket, and MT5 comment. Started: the execution client now persists an
  adapter-local reconciliation payload through the Nautilus generic cache.
- Recover client order IDs after node restart. Started: cached MT5 comment and
  venue-order mappings are loaded after account discovery and used when parsing
  order and fill reports.
- Reconcile orders that were submitted with uncertain `order_send` outcome.
  Started: the MT5 comment-to-client-order mapping is persisted before
  `order_send`, so later active/history reports can recover the client order ID
  even when the immediate submit outcome was unknown.
- Handle broker-specific history windows and missing historical records.
  Started: the default reconciliation history lookback is configurable through
  `reconciliation_lookback_days`.
- Add reconciliation tests using fake MT5 responses. Started: helper tests cover
  comment/order/deal ticket mapping and cache-byte round trips.
- Remaining: add full fake-bridge tests for active orders, historical orders,
  historical deals, and submit-result uncertainty.
- Remaining: decide whether MT5 mappings should be compacted, expired, or
  archived after terminal orders are fully reconciled.
- Remaining: document cache database requirements for restart recovery in live
  MT5 deployments.

### Phase 5 - Market Data Quality

Status: started.

- Deduplicate polled ticks and bars. Started: live quote polling, live bar
  polling, historical quote requests, and historical bar requests now suppress
  exact duplicate market data when enabled.
- Emit `TradeTick` where MT5 tick flags and broker data make this reliable.
- Support market depth where `market_book_add` / `market_book_get` are
  available. Started: async bridge methods exist for `market_book_add`,
  `market_book_get`, and `market_book_release`.
- Improve bar polling so closed-bar and forming-bar behavior is explicit.
  Started: data config now exposes `emit_closed_bars`,
  `emit_forming_bars`, and `bar_poll_lookback_multiplier`.
- Add health checks for stale ticks and terminal disconnects. Started: quote
  and bar polling emit rate-limited warnings for disconnected terminals, missing
  ticks/rates, and stale tick timestamps.
- Remaining: add `TradeTick` parsing only when MT5 tick flags and broker data
  can reliably distinguish real trades from quote changes.
- Remaining: wire market depth into Nautilus order book data messages and test
  broker support with `market_book_get` snapshots.
- Remaining: add fake-bridge tests for duplicate ticks, duplicate bars,
  closed/forming bar selection, and stale tick warning behavior.

### Phase 6 - Runtime And Operations

Status: started.

- Add reconnect, re-login, and backoff behavior. Started: the MT5 bridge can
  check terminal health through `terminal_info`, reconnect, re-login, and retry
  with bounded backoff. Data and execution clients call this before live polling,
  account updates, order commands, and reconciliation report requests.
- Add terminal health monitoring. Started: data and execution clients can run a
  background terminal health monitor controlled by `monitor_terminal_health` and
  `terminal_health_check_interval_ms`.
- Add optional dependency packaging guidance for `MetaTrader5`. Started: runtime
  notes below document that the official `MetaTrader5` Python package should be
  installed only in environments that can load the terminal integration.
- Add integration-test fixtures around a fake bridge. Started: unit-level fake
  MT5 module tests cover healthy-terminal and reconnect behavior for the bridge.
- Add demo-account test procedure for Windows or a compatible terminal runtime.
  Started: runtime notes below list the minimum demo-account procedure.
- Document limitations for macOS/Linux where the official MT5 terminal/Python
  package may require a broker-supported setup. Started: runtime notes below
  describe the supported-runtime constraint.
- Remaining: add long-running integration tests that simulate terminal drop,
  login failure, order-send timeout, and recovery across data and execution
  clients.
- Remaining: expose richer terminal health state through metrics when the live
  runtime has a metrics sink available.

## Runtime Operations Notes

The MT5 adapter is intentionally implemented as an optional live adapter. The
official `MetaTrader5` Python package is imported lazily by `MT5TerminalBridge`,
so environments that do not trade through MT5 do not need the package installed.
Live MT5 deployments should install the package in the same Python environment
as the NautilusTrader node and ensure the terminal can be launched or attached
through the configured `path`, `login`, `password`, and `server` settings.

For restart-safe reconciliation, configure a Nautilus cache database. Without a
cache backing store, MT5 comment/order/deal mappings are retained only for the
current process lifetime.

The minimum demo-account procedure is:

- Start a compatible MT5 terminal and log into the demo account.
- Run the data smoke test for target symbols and verify ticks and bars are fresh.
- Run guarded execution tests for market, limit, stop-market, stop-limit,
  modify, cancel, rejection, partial close, and close-by where the account mode
  and broker support them.
- Restart the Nautilus node and verify open orders, fills, and positions are
  reconciled with the same `ClientOrderId` and MT5 tickets.
- Disconnect or close the terminal, confirm reconnect warnings are emitted, then
  restore the terminal and verify polling/account reports recover.

The official MT5 Python package is primarily distributed for Windows terminal
workflows. macOS/Linux deployments may require a broker-supported terminal setup,
Wine/containerization, or a remote Windows runtime. Treat those environments as
deployment-specific until demo-account tests confirm terminal startup, login,
market data, order routing, and reconnect behavior.

## Acceptance Criteria

The adapter should not be considered production-ready until:

- It has passed demo-account tests for market, limit, stop-market, stop-limit,
  modify, cancel, partial fill, full fill, and rejection scenarios.
- Reconciliation works after restart for open orders and positions.
- Netting and hedging account behavior is explicitly tested.
- Broker symbol rule validation prevents invalid requests before `order_send`.
- Terminal disconnect/reconnect behavior is tested.
- Position/account reports match MT5 terminal values within expected rounding.
