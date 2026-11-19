# DownloadX speed engine: implementation and validation plan

This document is the working engineering contract for the DownloadX multi-connection work.
It deliberately starts from the existing engine rather than replacing its planner or retry logic.

## Existing behaviour to preserve

- HTTP/HTTPS and FTP/FTPS transfers use the Rust engine in `crates/engine`.
- HTTP downloads use one client/pool per download and one in-flight request per worker.
- Range support is confirmed by a valid partial response; servers that ignore ranges fall back to one stream.
- `Options::max_connections` defaults to 8 and is clamped to 1..=128 (the normal UI ceiling is 32).
- The planner assigns, preempts and splits byte ranges while preventing overlapping writes.
- Slow-connection shedding is opt-in through `Options::shed_slow` (enabled by default), and is suppressed when a speed limiter, request cap or disk backpressure makes its measurements misleading.
- Per-host connection limits, retry handling, resume validators and overlap verification must remain intact.

## DownloadX speed modes

The user-facing mode is a policy layered over the existing engine. It must not bypass server capability detection or host limits.

### Auto (default)
- Start conservatively using the user's configured connection ceiling and the engine's file-size / host-cap checks.
- Increase parallelism only after measured throughput improves without a sustained rise in RTT, retry rate or timeout rate.
- Decrease parallelism quickly after congestion, repeated throttling or connection failures; increase it slowly after a stable interval.
- Keep the current slow-worker recovery, but do not shed workers while the disk or an explicit speed limit is the bottleneck.

### Maximum Speed
- Permit faster additive increases up to the configured ceiling when the server accepts ranges and the network is not congested.
- Retain hard limits from the server, host-limit learning, memory budget, disk backpressure and the user's global/per-download limits.
- Never claim speed beyond the server, route, Wi-Fi/mobile link or disk capacity.

### Stable Speed
- Prefer fewer simultaneous workers and longer observation windows.
- Penalize retry bursts, RTT spikes and short-lived throughput gains.
- Avoid frequent connection churn; keep a slow worker if reconnecting is unlikely to improve completion time.

## Telemetry required before changing concurrency

Collect per-download, bounded-window measurements:
- aggregate bytes/sec (smoothed and instantaneous);
- per-worker bytes/sec and time spent receiving vs waiting for disk;
- request RTT / time-to-first-byte;
- active, receiving, retrying and retired worker counts;
- HTTP status / transport error counts, especially 429, 503, timeout and reset;
- server range support and learned per-host connection cap;
- disk write / flush wait signals and limiter state.

Telemetry must be bounded, cheap, and reset when a new download starts. Do not persist cookies, authorization headers, or full signed URLs in diagnostics.

## Adaptive controller guardrails

- Use a smoothed throughput estimator (EWMA) for display and a separate short-window signal for congestion detection.
- Use additive increase and multiplicative decrease with hysteresis; never change the worker count on every UI refresh.
- Require multiple healthy observation windows before increasing concurrency.
- Reduce concurrency on repeated 429/503, rising RTT plus falling throughput, or repeated connect/read timeouts.
- Do not interpret a single slow worker as whole-link congestion.
- Do not increase workers when ranges are unsupported, a host cap is known, a limiter is active, or disk backpressure is active.
- Never exceed the configured user ceiling or `MAX_CONNECTIONS_UNLOCKED`.
- Controller decisions must be deterministic under a fake clock / synthetic telemetry so they can be unit-tested.

## Benchmark matrix and acceptance criteria

Compare the unmodified baseline with each controller mode using identical server fixtures and file sizes.

| Scenario | What it checks |
| --- | --- |
| Local fast range server | Maximum throughput and CPU overhead |
| Per-connection server cap | Whether extra workers help or just add overhead |
| High RTT / low loss | Slow ramp and useful parallelism |
| Injected packet loss / resets | Recovery and retry stability |
| Periodically slow worker | Avoiding whole-download stalls |
| 429 / 503 with Retry-After | Backoff and server friendliness |
| No Range support / server returns 200 | Correct single-stream fallback |
| Range server returns malformed 206 / 416 | No corrupt or overlapping writes |
| Slow disk / long flush | No false congestion response |
| Pause, resume and process restart | Correct validators and byte continuity |
| Network changes during transfer | Recovery without duplicate writes |

Record median throughput, p95 throughput, speed coefficient of variation, completion time, retry count, failed requests, CPU and peak memory. A change is accepted only when correctness tests pass and it improves the target mode without a material regression in the other modes.

## Rollout order

1. Add deterministic controller and telemetry unit tests without changing defaults.
2. Add the controller behind an explicit internal feature flag and compare against the existing supervisor.
3. Run the benchmark matrix and tune thresholds from results, not guesses.
4. Expose Auto / Maximum Speed / Stable Speed in the app settings and per-download controls.
5. Enable Auto by default only after Windows CI and benchmark results are clean.

## Branding and compatibility notes

- Keep the existing installer AppId, mutex, registry keys, app-data directory and browser native-messaging identifiers during the first branding rollout. Existing installations and browser integrations depend on them.
- Keep original upstream attribution and GPL-3.0-or-later notices.
- Rename executable and release artifact filenames only in one coordinated change with the updater, installer, smoke tests, browser host manifests, CI and release workflows. A partial rename can make upgrades or browser integration fail.

## Live-supervisor integration audit (2026-11-19)

The first live integration must not be implemented as a simple comparison between worker IDs and a desired count.

- `supervise()` currently launches workers into a Tokio `JoinSet` and assigns monotonically increasing `WorkerId` values.
- A worker repeatedly asks the planner for its next assignment. Returning from a worker is currently tied to planner completion, range capability, retry exhaustion, or errors; there is no independent per-worker retirement signal.
- The current cancellation token is shared by the whole download. Cancelling it to reduce parallelism would stop every worker, not just excess workers.
- Worker IDs are identifiers for planner ownership and progress views, not reusable concurrency slots. A target-count check against `next_id` would therefore be incorrect after workers retire.
- The planner already exposes active and receiving worker counts, while `ConnView` exposes per-worker speed and status. The supervisor still needs bounded request-latency / response-outcome telemetry before the policy can safely react to network congestion.
- App settings are serialized with `#[serde(default)]`, and already persist the configured connection ceiling. A future mode preference can be added compatibly, but it should not be surfaced until the live engine actually consumes it.

### Cooperative retirement protocol to implement

The safe retirement point is the worker loop boundary after a complete segment, not an arbitrary point in an HTTP response.

1. Use a per-worker retirement request keyed by monotonic `WorkerId` in the planner, separate from the download-wide cancellation token. The planner only lets the worker consume the request when it no longer owns a segment; this prevents retirement while any bytes remain claimed or uncommitted.
2. Have the `JoinSet` task return its `WorkerId` with the worker result so the supervisor can remove exactly the completed worker from the registry. Never treat `next_id` as the number of active workers; IDs remain monotonically increasing.
3. A worker checks its planner retirement request before asking for another assignment. A worker already inside `receive_all` is allowed to finish its current segment. `Writer::wait` has then awaited the disk write and called `Planner::commit`; the worker can return without abandoning buffered bytes or releasing an uncommitted claim.
4. If a worker is between segments after a preemption or retryable error, the existing path releases its planner claim before the next loop iteration; it can then retire at that boundary. Do not abort its task or cancel its HTTP body as a shortcut.
5. When the target falls, mark only the excess live workers for retirement (prefer the newest non-probe workers). When the target rises, spawn fresh workers with new IDs until the live-task count reaches the target. The supervisor continues to own respawn, host caps, range gating, file-size limits and the final completion check.
6. Keep the probe/connection-0 fallback invariant: no-range servers must continue with the original whole-file stream. A controller target of one must never cancel or retire the only worker that can finish a no-range download.
7. On global cancellation or completion, retain the existing shared-token shutdown path and await every worker. A retirement signal must never mask a fatal error or turn a cancelled download into success.



### Implemented groundwork (2026-11-19)

- `Planner` now records cooperative retirement requests by `WorkerId` and only consumes a request after that worker no longer owns a segment.
- `worker()` checks the request at the top of its assignment loop, after the previous segment's `Writer::wait` / `Planner::commit` path has completed or after a failed/preempted claim has been released. It then marks the connection finished and returns normally.
- Planner tests cover a retirement request during a partially committed segment (it remains pending until the full segment is committed) and retirement after a failed segment is released.
- At the time this primitive was added, the supervisor did not yet issue target-based retirement requests. The subsequent live integration is documented in the **Live controller integration** section below; it is still experimental pending full race tests and benchmarks.

### Telemetry still required for a live controller

- Count actual response outcomes at the request boundary, including status 429/503, transport failures, timeouts and Retry-After; supervisor task errors alone miss retries handled inside a worker.
- Measure request RTT/TTFB per response. `Snapshot.server.rtt` is a connection-open estimate, not a rolling per-request RTT signal, so it must not be misused as congestion telemetry.
- Sample aggregate received bytes over a monotonic bounded window; exclude windows in which `disk_held` says writes constrained the network.
- Derive active task count from the supervisor's live-worker registry, not planner ownership alone (a worker may be assigned while retrying).
- Treat unsupported ranges, known host caps, per-download/global limiters and disk backpressure as hard guardrails before applying any target.

### Retirement regression tests

Before enabling the controller, cover: retirement while receiving a segment; retirement while a disk write is pending; retirement after a preempted claim; retry then retirement; pause/cancel racing with retirement; final segment completion racing with target reduction; worker respawn after a prior worker exits; no-range probe preservation; resume-state byte continuity; and a many-worker randomized planner test proving every byte is committed at most once.



### Live controller integration (2026-11-19)

The first live Auto-mode integration is now present in `crates/engine/src/download.rs`:

- The supervisor wraps each `JoinSet` task with its monotonic `WorkerId` and tracks the live task IDs separately from the next ID counter.
- Every three seconds, and only after a valid `206 Partial Content` confirms range support, it samples deltas from the monotonic network-received byte counter and a bounded request window.
- HTTP/browser request telemetry records response-header latency (TTFB), transport errors, 429/503 throttling, 5xx responses and 408 timeouts. The latency sample queue is capped at 31 entries; each supervisor window drains its counters and calculates a median.
- The observation also includes current disk-held state and active per-download/global/request speed limits. The controller is bounded by the current configured/file-size ceiling and a freshly queried learned host cap.
- The supervisor requests retirement from newest non-probe workers first, cancels stale pending retirements when a newer window changes the target, and starts new workers with fresh IDs when the target rises.
- Worker tasks return their IDs with their results so the supervisor removes the exact completed task from its live registry.

### Current limitations and rollout gate

- Only `Auto` is selected by the live supervisor. `Maximum` and `Stable` remain policy/test modes; no app setting or per-download selector exists yet.
- The controller uses a per-download median response-header latency (TTFB), not a per-worker RTT distribution. This is a request-start-to-response-headers measurement, not a TCP RTT measurement. FTP's direct transfer path does not yet contribute request latency/status metrics.
- The initial connection count still follows the existing configured/file-size policy; adaptive reduction and later recovery are active only after range support is confirmed.
- `crates/engine/tests/adaptive.rs` now runs a range-supported, rate-limited 32 MiB transfer beyond a supervisor sampling interval, pauses it, resumes it, and verifies the completed file byte-for-byte. This exercises supervisor/worker shutdown and resume while the live controller is active. It does not yet assert exact target changes, worker respawn, host-cap changes during a transfer, or a pause/cancel racing at the precise retirement boundary.
- No reproducible before/after speed benchmark has been run on this integration. Do not claim a speed gain or make Auto the release default until the benchmark matrix and cross-platform checks pass.
- The engine CI suite now combines deterministic controller/planner tests with one live-supervisor pause/resume integration case. Further targeted tests are still needed for exact target changes, worker respawn, host-cap updates and retirement-boundary races under network and disk contention.

**Status:** the controller is connected to the live supervisor on the development branch, but it remains an experimental, unbenchmarked implementation pending integration/race tests, full CI, fuzzing and measured results.

**Integration gate:** the protocol and first telemetry/controller path are now implemented on the development branch. Run the engine suite, app tests, Windows/macOS/Linux builds, fuzzing and the benchmark matrix; add the missing supervisor race/integration tests before considering the controller production-ready. Until those checks pass, treat the live Auto controller as experimental and do not claim a measured speed improvement.
