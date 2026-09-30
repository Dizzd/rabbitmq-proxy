#!/usr/bin/env sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "install.sh must run as root" >&2
  exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

if [ "$#" -ge 1 ]; then
  BINARY=$1
elif [ -f "$PROJECT_DIR/rabbitmq-proxy" ]; then
  BINARY="$PROJECT_DIR/rabbitmq-proxy"
else
  case "$(uname -m)" in
    x86_64)
      BINARY="$PROJECT_DIR/target/x86_64-unknown-linux-musl/release/rabbitmq-proxy"
      ;;
    i386|i486|i586|i686)
      BINARY="$PROJECT_DIR/target/i686-unknown-linux-musl/release/rabbitmq-proxy"
      ;;
    *)
      echo "Unsupported architecture: $(uname -m). Pass the binary path explicitly." >&2
      exit 1
      ;;
  esac
fi

if [ ! -f "$BINARY" ]; then
  echo "Binary not found: $BINARY" >&2
  exit 1
fi

if ! id rabbitmq-proxy >/dev/null 2>&1; then
  useradd --system --home /nonexistent --shell /usr/sbin/nologin rabbitmq-proxy
fi

install -d -m 0755 -o root -g root /opt/rabbitmq-proxy /etc/rabbitmq-proxy
install -d -m 0750 -o rabbitmq-proxy -g rabbitmq-proxy /var/log/rabbitmq-proxy
install -m 0755 -o root -g root "$BINARY" /opt/rabbitmq-proxy/rabbitmq-proxy

if [ ! -f /etc/rabbitmq-proxy/config.yml ]; then
  install -m 0640 -o root -g rabbitmq-proxy "$PROJECT_DIR/config.example.yml" /etc/rabbitmq-proxy/config.yml
else
  echo "Preserving existing /etc/rabbitmq-proxy/config.yml"
fi

install -m 0644 "$PROJECT_DIR/systemd/rabbitmq-proxy-listener.service" /etc/systemd/system/rabbitmq-proxy-listener.service
install -m 0644 "$PROJECT_DIR/systemd/rabbitmq-proxy-forwarder.service" /etc/systemd/system/rabbitmq-proxy-forwarder.service
systemctl daemon-reload

echo "Installed rabbitmq-proxy. Review /etc/rabbitmq-proxy/config.yml before enabling services."
