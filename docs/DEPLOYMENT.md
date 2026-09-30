# Production Deployment Without Cloning the Repository

This guide installs `rabbitmq-proxy` directly from a GitHub Release. The production server does not need Git, Rust, Cargo, a C compiler, or the project source code.

## Release contents

Each tagged GitHub Release contains static musl binaries for both supported x86 architectures:

| `uname -m` result | Release asset |
|---|---|
| `x86_64` | `rabbitmq-proxy-vX.Y.Z-linux-x86_64-musl.tar.gz` |
| `i386`, `i486`, `i586`, or `i686` | `rabbitmq-proxy-vX.Y.Z-linux-i686-musl.tar.gz` |

The archive contains:

```text
rabbitmq-proxy/
├── rabbitmq-proxy
├── config.yml
├── config.example.yml
├── README.md
├── docs/
│   └── DEPLOYMENT.md
├── logrotate/
│   └── rabbitmq-proxy
├── scripts/
│   ├── install.sh
│   └── uninstall.sh
└── systemd/
    ├── rabbitmq-proxy-listener.service
    └── rabbitmq-proxy-forwarder.service
```

Use the `.tar.gz` archive when running `scripts/install.sh`. The standalone binary asset is provided for manual/custom installations and does not contain the configuration template, systemd units, logrotate policy, or scripts.

## Prerequisites

The server needs:

- Ubuntu with `systemd`, including Ubuntu 18.04.
- Root or `sudo` access for installation.
- Network access to RabbitMQ and the PMS/EWS target.
- `curl`, `tar`, `grep`, and `sha256sum` for online installation.
- `logrotate`, which is normally installed by default on Ubuntu.
- An existing RabbitMQ topology when `rabbitmq.declare_topology` is `false`.

The musl binaries do not require a compatible host glibc version or a system OpenSSL installation.

## 1. Select the correct architecture

Run on the production server:

```bash
uname -m
```

For the known Ubuntu 18.04 EWS machine reporting `i686`, use:

```bash
ARCH=i686
```

For a 64-bit x86 machine reporting `x86_64`, use:

```bash
ARCH=x86_64
```

An `x86_64` executable cannot run on an `i686` operating system.

## 2. Download a GitHub Release

Set the required release version and select the architecture reported by `uname -m`:

```bash
REPOSITORY="Dizzd/rabbitmq-proxy"
VERSION="v1.0.1"
ARCH="i686"
ASSET="rabbitmq-proxy-${VERSION}-linux-${ARCH}-musl"
BASE_URL="https://github.com/${REPOSITORY}/releases/download/${VERSION}"

curl -fLO "${BASE_URL}/${ASSET}.tar.gz"
curl -fLO "${BASE_URL}/SHA256SUMS"
```

Verify that the archive matches the checksum published by GitHub Actions:

```bash
grep -F "  ${ASSET}.tar.gz" SHA256SUMS | sha256sum -c -
```

Expected output:

```text
rabbitmq-proxy-v1.0.1-linux-i686-musl.tar.gz: OK
```

Do not install the archive if checksum verification fails.

## 3. Extract and inspect

```bash
tar -xzf "${ASSET}.tar.gz"
cd rabbitmq-proxy
file rabbitmq-proxy
./rabbitmq-proxy --version
./rabbitmq-proxy check-config
```

The `check-config` command uses `./config.yml` when `--config` is omitted.

For an i686 release, `file rabbitmq-proxy` should report a 32-bit x86 ELF executable. For x86_64 it should report a 64-bit x86-64 ELF executable. Both release variants should be statically linked.

## 4. Install files and systemd units

Run the packaged installer:

```bash
sudo ./scripts/install.sh
```

Do not copy `scripts/install.sh` away from the extracted directory. It resolves `config.example.yml`, `systemd/`, and `logrotate/` relative to its own location and now exits before making changes when required package files are missing.

The installer creates the service user and installs:

```text
/opt/rabbitmq-proxy/rabbitmq-proxy
/etc/rabbitmq-proxy/config.yml
/var/log/rabbitmq-proxy/
/etc/logrotate.d/rabbitmq-proxy
/etc/systemd/system/rabbitmq-proxy-listener.service
/etc/systemd/system/rabbitmq-proxy-forwarder.service
```

If `/etc/rabbitmq-proxy/config.yml` or `/etc/logrotate.d/rabbitmq-proxy` already exists, the installer preserves it.

## 5. Configure production

Edit the installed configuration:

```bash
sudo editor /etc/rabbitmq-proxy/config.yml
```

At minimum, verify:

- Listener bind address, port, path, allowed source IPs, body limit, and timeout.
- RabbitMQ host, port, virtual host, username, password, exchange, routing key, and queue.
- PMS/EWS target URL and HTTP timeouts.
- Retry, poison-message, prefetch, and concurrency settings.
- Log directory and health ports.
- `rabbitmq.declare_topology` policy.

For an existing production RabbitMQ topology, keep:

```yaml
rabbitmq:
  declare_topology: false
```

With this setting the application never declares or modifies the exchange, queue, or binding. They must already exist.

To let the application create a durable direct exchange, durable queue, and routing-key binding when missing, explicitly set:

```yaml
rabbitmq:
  declare_topology: true
```

The application does not create a DLX or DLQ. A `dead_letter` poison-message strategy requires existing RabbitMQ dead-letter configuration.

Validate the installed file before starting either service:

```bash
sudo -u rabbitmq-proxy \
  /opt/rabbitmq-proxy/rabbitmq-proxy check-config \
  --config /etc/rabbitmq-proxy/config.yml
```

Expected output:

```text
Config OK
```

## 6. Start production services

Enable and start both independent services:

```bash
sudo systemctl enable --now rabbitmq-proxy-listener.service
sudo systemctl enable --now rabbitmq-proxy-forwarder.service
```

Check their state:

```bash
sudo systemctl status rabbitmq-proxy-listener.service --no-pager
sudo systemctl status rabbitmq-proxy-forwarder.service --no-pager
```

The listener unit has `CAP_NET_BIND_SERVICE`, allowing the unprivileged `rabbitmq-proxy` user to bind a configured port below 1024, such as port 85.

## 7. Verify health and readiness

Using the default health addresses:

```bash
curl -i http://127.0.0.1:9085/health
curl -i http://127.0.0.1:9085/ready
curl -i http://127.0.0.1:9086/health
curl -i http://127.0.0.1:9086/ready
```

`/health` reports whether the service process and health server are running. `/ready` returns HTTP 200 only when the corresponding RabbitMQ publisher or consumer is connected; otherwise it returns HTTP 503.

## Logs and troubleshooting

Follow systemd logs:

```bash
sudo journalctl -u rabbitmq-proxy-listener.service -f
sudo journalctl -u rabbitmq-proxy-forwarder.service -f
```

Inspect configured log files:

```bash
sudo tail -f /var/log/rabbitmq-proxy/listener.log
sudo tail -f /var/log/rabbitmq-proxy/forwarder.log
```

## Log rotation

The installer adds `/etc/logrotate.d/rabbitmq-proxy` unless that file already exists. The default policy:

- Checks the logs daily and rotates files larger than 50 MiB.
- Keeps 14 rotations.
- Compresses older rotations.
- Uses `copytruncate`, so the services do not need to restart or reopen their log file descriptors.
- Runs rotation as the `rabbitmq-proxy` user and group.

Validate the syntax without rotating logs:

```bash
sudo logrotate --debug /etc/logrotate.d/rabbitmq-proxy
```

To force a one-time rotation test:

```bash
sudo logrotate --force /etc/logrotate.d/rabbitmq-proxy
```

The packaged policy covers the default `/var/log/rabbitmq-proxy/*.log` path. If `logging.directory`, `listener_file`, or `forwarder_file` is changed, update `/etc/logrotate.d/rabbitmq-proxy` to match. `copytruncate` can lose a very small number of log records written between the copy and truncate operations; application traffic and RabbitMQ delivery are unaffected.

Useful checks:

```bash
sudo -u rabbitmq-proxy test -r /etc/rabbitmq-proxy/config.yml
sudo ss -lntp | grep -E ':85|:9085|:9086'
sudo systemctl restart rabbitmq-proxy-listener.service
sudo systemctl restart rabbitmq-proxy-forwarder.service
```

If readiness remains HTTP 503, check RabbitMQ connectivity, credentials, virtual host permissions, exchange/queue names, and whether topology declaration is enabled.

## Offline deployment

For a server without internet access, download these two files on another machine:

```text
rabbitmq-proxy-vX.Y.Z-linux-ARCH-musl.tar.gz
SHA256SUMS
```

Copy them to the server using the approved transfer mechanism, such as SCP or removable media. Then continue from checksum verification in step 2. No source repository is required.

## Upgrade without cloning source

Download and verify the newer release archive, then extract it into a new temporary directory. The production configuration is preserved by the installer.

```bash
sudo systemctl stop rabbitmq-proxy-listener.service
sudo systemctl stop rabbitmq-proxy-forwarder.service

cd rabbitmq-proxy
sudo ./scripts/install.sh

sudo -u rabbitmq-proxy \
  /opt/rabbitmq-proxy/rabbitmq-proxy check-config \
  --config /etc/rabbitmq-proxy/config.yml

sudo systemctl start rabbitmq-proxy-listener.service
sudo systemctl start rabbitmq-proxy-forwarder.service
```

Confirm the installed version and readiness:

```bash
/opt/rabbitmq-proxy/rabbitmq-proxy --version
curl -i http://127.0.0.1:9085/ready
curl -i http://127.0.0.1:9086/ready
```

## Rollback

Keep the previously verified release archive. To roll back, extract the older archive and run its `scripts/install.sh`, then restart both services. The existing production configuration remains unchanged.

## Uninstall

From any extracted release directory:

```bash
sudo ./scripts/uninstall.sh
```

The uninstall script removes the executable, systemd units, and logrotate policy but preserves `/etc/rabbitmq-proxy/config.yml` and `/var/log/rabbitmq-proxy`.

## Production checklist

- Correct release architecture selected with `uname -m`.
- SHA-256 checksum verified before installation.
- `check-config` succeeds as the service user.
- Production RabbitMQ topology policy is intentional.
- RabbitMQ credentials are restricted to the required virtual host and operations.
- Listener `allowed_ips` contains only trusted source addresses.
- Payload logging is disabled unless explicitly required.
- Both systemd services are enabled and active.
- Both `/ready` endpoints return HTTP 200.
- At-least-once duplicate delivery behavior is understood by the PMS/EWS owner.
