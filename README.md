# rabbitmq-proxy

`rabbitmq-proxy` is one Rust executable with two independent production service modes and one shared YAML configuration:

```text
rabbitmq-proxy listener
rabbitmq-proxy forwarder
```

For development or small deployments, `rabbitmq-proxy all` runs both services as independent Tokio tasks in one process.

For a production installation directly from GitHub Release without cloning source code, see [Production Deployment](docs/DEPLOYMENT.md).

## Architecture

```mermaid
flowchart LR
    N[Nomadix / HTTP Source]
    L[rabbitmq-proxy listener]
    R[(RabbitMQ)]
    F[rabbitmq-proxy forwarder]
    P[PMS / EWS]

    N -->|HTTP POST raw bytes| L
    L -->|AMQP publish| R
    R -->|consume manual ACK| F
    F -->|HTTP POST raw bytes| P
```

The listener and forwarder do not parse or transcode payloads. They use `bytes::Bytes`/raw byte buffers end to end, so control characters and non-UTF-8 payloads are preserved.

## Build

Requirements: Rust stable 1.88 or newer.

```bash
cargo build --release
```

The project declares exactly one Cargo binary:

```text
target/release/rabbitmq-proxy
```

Validation commands:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo build --release
```

## Static Linux build

Both Reqwest and Lapin use rustls. The project does not require the host OpenSSL library.

On Ubuntu/Debian with a musl C toolchain:

```bash
sudo apt-get update
sudo apt-get install -y musl-tools
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
file target/x86_64-unknown-linux-musl/release/rabbitmq-proxy
ldd target/x86_64-unknown-linux-musl/release/rabbitmq-proxy || true
```

Output:

```text
target/x86_64-unknown-linux-musl/release/rabbitmq-proxy
```

A musl-linked binary avoids depending on the glibc version installed by Ubuntu. This is the recommended artifact for one x86_64 binary spanning Ubuntu 18.04 through newer releases. Test the final artifact on every supported OS image before release; kernel, DNS, CA-certificate, and local security policy differences still apply.

The release workflow also builds `i686-unknown-linux-musl` for 32-bit Ubuntu installations. Check the machine architecture before downloading:

```bash
uname -m
```

Use the `linux-i686-musl` asset when the result is `i686`, `i586`, or another 32-bit x86 identifier. An `x86_64` binary cannot run on an `i686` operating system.

## GitHub releases

The workflow in `.github/workflows/release.yml` runs only when a semantic version tag matching `vMAJOR.MINOR.PATCH` is pushed. The tag version must match `package.version` in `Cargo.toml`.

```bash
# Update the package version before tagging when necessary.
git add Cargo.toml Cargo.lock
git commit -m "chore: release v1.0.2"
git tag v1.0.2
git push origin main
git push origin v1.0.2
```

GitHub Actions runs formatting, Clippy, tests, and static musl builds. It creates a GitHub Release containing:

```text
rabbitmq-proxy-v1.0.2-linux-x86_64-musl
rabbitmq-proxy-v1.0.2-linux-x86_64-musl.tar.gz
rabbitmq-proxy-v1.0.2-linux-i686-musl
rabbitmq-proxy-v1.0.2-linux-i686-musl.tar.gz
SHA256SUMS
```

Each archive contains the executable, a starting `config.yml`, `config.example.yml`, README, the production deployment guide, systemd units, a logrotate policy, and installation scripts.

Use the `.tar.gz` archive for production installation. The standalone binary asset contains only the executable and does not include `config.example.yml`, systemd units, logrotate policy, or installation scripts.

## CLI

```bash
rabbitmq-proxy listener --config /etc/rabbitmq-proxy/config.yml
rabbitmq-proxy forwarder --config /etc/rabbitmq-proxy/config.yml
rabbitmq-proxy all --config config.yml
rabbitmq-proxy check-config --config config.yml
rabbitmq-proxy --version
rabbitmq-proxy --help
```

When `--config` is omitted, the default path is `./config.yml` relative to the current working directory:

```bash
./rabbitmq-proxy listener
./rabbitmq-proxy forwarder
```

The binary does not embed a production configuration. If `./config.yml` is missing or invalid, startup fails with a clear non-zero error. Release archives include `config.example.yml` copied as `config.yml`, so an extracted release has an editable starting configuration.

`check-config` prints `Config OK` and exits zero for a valid file. Invalid or missing configuration prints `Config ERROR: ...` as part of the error and exits non-zero.

## Configuration

Copy `config.example.yml` and change every environment-specific value. No address, credential, URL, exchange, routing key, queue, retry value, or external-I/O timeout is compiled into the service configuration.

Important defaults and behavior:

- `rabbitmq.declare_topology: false` means no exchange declaration, queue declaration, or binding is performed.
- `app.shutdown_timeout_seconds` bounds graceful draining of active forwarder jobs.
- When topology declaration is enabled, a durable direct exchange, durable queue, and configured binding are declared.
- `listener.allowed_ips` contains TCP peer IP addresses. `X-Forwarded-For` is never trusted.
- `logging.log_payload` is false by default. When enabled, payloads are logged as uppercase hex, truncated at `max_payload_log_bytes`.
- Credentials are used to construct the AMQP connection URI but are never included in application logs.

Run validation before deploying a change:

```bash
./rabbitmq-proxy check-config --config /etc/rabbitmq-proxy/config.yml
```

## Listener mode

The listener binds `listener.bind:listener.port` and accepts POST requests only at `listener.path`:

1. Read the real TCP peer address.
2. Reject peers not in `allowed_ips` with 403.
3. Enforce `max_body_size` without converting the body to text.
4. Publish through one persistent RabbitMQ connection/channel.
5. Wait for a RabbitMQ publisher confirmation.
6. Return 200 `OK` only after an ACK confirmation.

Responses are 403 for denied IPs, 413 for an oversized body, 503 while RabbitMQ is unavailable or confirmation times out, and 400 for malformed HTTP rejected by the HTTP server. Unexpected server failures are logged and do not expose internal details.

RabbitMQ being down at startup does not prevent the HTTP and health servers from running. Requests receive 503 until the publisher becomes ready.

## Forwarder mode

The forwarder creates one persistent Reqwest client and consumes with manual acknowledgement:

1. RabbitMQ delivers a message under the configured QoS prefetch.
2. The worker POSTs the unchanged raw bytes to `forwarder.target_url`.
3. HTTP 2xx is considered successful.
4. The delivery is ACKed only after success.

Transport errors, 408, 429, and 5xx responses are retryable. Other 4xx responses are permanent by default. Retries use bounded exponential backoff.

`forwarder.concurrency: 1` gives the best practical ordering. Values greater than one improve throughput but completion and ACK order are not guaranteed. A concurrency change is validated on reload but requires a restart because the worker semaphore is created at service startup.

## Manual ACK, retry, and poison messages

The effective in-process attempt count is the smaller of `retry.max_attempts` and `poison_message.max_attempts`. After retry exhaustion:

- `drop_and_log`: ACK and log the discarded message. This is the safe default against infinite redelivery.
- `requeue`: NACK with `requeue=true`. This can redeliver indefinitely because attempt state is not stored across RabbitMQ deliveries; use it deliberately.
- `dead_letter`: NACK with `requeue=false`. RabbitMQ routes the message only if the existing queue has a dead-letter exchange policy/argument. The proxy logs a clear error when it cannot guarantee that topology. It never creates a DLQ implicitly.

## At-least-once semantics

Delivery is at least once, not exactly once. If the PMS accepts a POST and the network or process fails before RabbitMQ receives the ACK, RabbitMQ can redeliver the message and the PMS can receive a duplicate.

The forwarder keeps AMQP properties available at the delivery boundary, providing an extension point for a future `Idempotency-Key` or `X-Message-ID`. The current implementation does not add headers and never changes the payload.

## RabbitMQ reconnect

Publisher and consumer modes have independent reconnect loops. A lost connection marks readiness false, logs the disconnect, and retries with exponential delays capped by `rabbitmq.reconnect.max_delay_ms`. External operations have bounded timeouts and reconnect loops observe graceful shutdown.

Publisher connections and channels are persistent; no connection is created per HTTP request. Publisher confirms are enabled once per channel. Consumer connections configure `basic_qos` and manual ACK after every reconnect.

## Dynamic configuration

The process watches the directory containing `config.yml`, which also supports atomic file replacement by configuration-management tools. On change it:

1. Loads and deserializes the new file.
2. Runs complete validation.
3. Reloads the tracing level.
4. Atomically replaces the active snapshot.

Invalid files are logged and ignored; the previous valid configuration remains active.

Read/open/close filesystem events are ignored, so reading `config.yml` cannot trigger a reload loop. Create, modify, remove, rename, and watcher-rescan events are debounced, and a valid configuration equal to the active snapshot is treated as unchanged without emitting `configuration_reloaded`.

Hot-applied fields include allowed IPs, listener body/request limits, target URL, forwarder request timeout, retry/poison settings, reconnect delays, payload logging, and log level. Bind addresses, ports, listener path, file paths, forwarder connect timeout/concurrency, and RabbitMQ topology/consumer setup produce a `configuration_requires_restart` warning. Some RabbitMQ connection fields may be observed on a later reconnect, but a controlled restart is required for deterministic rollout.

## Logging

Logs are newline-delimited JSON via `tracing`. Listener mode writes `logging.listener_file`; forwarder mode writes `logging.forwarder_file`; `all` routes forwarder targets to the forwarder file and other service events to the listener file. Logs are also emitted to stdout for journald.

If the log directory is temporarily unavailable, startup continues with stdout logging and emits `file_logging_unavailable`.

The installer adds `/etc/logrotate.d/rabbitmq-proxy`. The default policy rotates `*.log` daily or when a file exceeds 50 MiB, keeps 14 rotations, and compresses old logs. It uses `copytruncate` because the running process keeps its log file descriptor open. If `logging.directory` or either configured filename is changed, update the logrotate path as well. Validate the installed policy with:

```bash
sudo logrotate --debug /etc/logrotate.d/rabbitmq-proxy
```

## Health checks

Listener health server, normally `127.0.0.1:9085`:

```text
GET /health  -> 200 while the process/server is running
GET /ready   -> 200 when the publisher is connected, otherwise 503
```

Forwarder health server, normally `127.0.0.1:9086`:

```text
GET /health  -> 200 while the process/server is running
GET /ready   -> 200 when the RabbitMQ consumer is connected, otherwise 503
```

PMS availability does not make `/health` fail. It is handled through retry and poison-message policy.

## Graceful shutdown

SIGINT and SIGTERM cancel all service loops. Axum stops accepting new requests and drains in-flight requests. The forwarder stops taking new deliveries, gives active jobs up to `app.shutdown_timeout_seconds` to finish and ACK, then aborts unfinished jobs so their unacknowledged deliveries can be redelivered. AMQP channels and connections are closed cleanly when available. Non-blocking log guards remain alive until service shutdown.

## Systemd installation

Build a musl release, then run:

```bash
sudo ./scripts/install.sh target/x86_64-unknown-linux-musl/release/rabbitmq-proxy
sudo editor /etc/rabbitmq-proxy/config.yml
sudo systemctl enable --now rabbitmq-proxy-listener.service
sudo systemctl enable --now rabbitmq-proxy-forwarder.service
```

From a downloaded GitHub Release archive, the installer automatically uses the packaged binary, so no binary argument is required:

```bash
tar -xzf rabbitmq-proxy-v1.0.2-linux-i686-musl.tar.gz
cd rabbitmq-proxy
sudo ./scripts/install.sh
```

The installer creates the `rabbitmq-proxy` system user, required directories, one executable, two units, a logrotate policy, and a config only when one does not already exist. It never overwrites an existing production `config.yml` or logrotate policy.

Installed layout:

```text
/opt/rabbitmq-proxy/rabbitmq-proxy
/etc/rabbitmq-proxy/config.yml
/var/log/rabbitmq-proxy/listener.log
/var/log/rabbitmq-proxy/forwarder.log
/etc/logrotate.d/rabbitmq-proxy
/etc/systemd/system/rabbitmq-proxy-listener.service
/etc/systemd/system/rabbitmq-proxy-forwarder.service
```

Uninstall while preserving config and logs:

```bash
sudo ./scripts/uninstall.sh
```

## Development integration environment

`docker-compose.yml` starts RabbitMQ with its management UI and a raw HTTP echo target. The development `config.yml` enables topology declaration. Start dependencies, run `rabbitmq-proxy all`, then POST raw bytes to the listener:

```bash
docker compose up -d
cargo run -- all --config config.yml
printf '\0024189392543\r\n\003' | curl --data-binary @- http://127.0.0.1:8085/
```

## Failure scenarios

| Scenario | Behavior |
|---|---|
| RabbitMQ down before listener starts | Listener stays up, readiness is 503, background reconnect continues, POST returns 503. |
| RabbitMQ restarts | Publisher/consumer readiness becomes false and each loop reconnects with backoff. |
| Disconnect during publish | Publish/confirm is bounded; request returns 503 and connection recovery proceeds. |
| PMS down, DNS failure, reset, or timeout | No ACK; bounded retry runs, followed by poison policy. |
| PMS returns 500 | Retry. |
| PMS returns 400 | No retry; apply poison policy. |
| Invalid config on reload | Log the validation error and retain the old snapshot. |
| SIGTERM | Stop intake, drain bounded in-flight work, close resources, flush logs. |

## Troubleshooting

- Check syntax and validation first with `rabbitmq-proxy check-config --config ...`.
- Check `/ready`; `/health` only proves that the service process and health server are alive.
- Verify the exchange, routing key, queue, permissions, and virtual host already exist when `declare_topology` is false.
- A 503 from the listener usually means publisher readiness/confirmation failure; inspect `rabbitmq_connection_failed` and `rabbitmq_publish_failed` events.
- Repeated forwarder failures include `delivery_tag`, attempt, HTTP status or transport error, and the selected poison outcome.
- With `dead_letter`, configure the DLX/DLQ in RabbitMQ before deployment. The application does not mutate production topology by default.
- For ordering-sensitive integrations keep `concurrency: 1` and remember that RabbitMQ redelivery can still produce duplicates.

## Ubuntu compatibility

The recommended release artifacts are `x86_64-unknown-linux-musl` for 64-bit x86 systems and `i686-unknown-linux-musl` for legacy 32-bit x86 systems, built with rustls and no system OpenSSL dependency. This avoids building on a new Ubuntu glibc and accidentally requiring a glibc version unavailable on Ubuntu 18.04. The two systemd services use features available on supported Ubuntu systemd releases.

The support promise should be verified in CI or release testing with Ubuntu 18.04, 20.04, 22.04, 24.04, and the final 26.04 release image. At the time of writing, 26.04 compatibility is a build/runtime target and must be confirmed against its released userspace.

## Contributors

- [Dizzd](https://github.com/Dizzd) - project owner and maintainer.

See [CONTRIBUTORS.md](CONTRIBUTORS.md) for contributor acknowledgements.
