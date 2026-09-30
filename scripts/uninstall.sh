#!/usr/bin/env sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "uninstall.sh must run as root" >&2
  exit 1
fi

systemctl disable --now rabbitmq-proxy-listener.service rabbitmq-proxy-forwarder.service 2>/dev/null || true
rm -f /etc/systemd/system/rabbitmq-proxy-listener.service
rm -f /etc/systemd/system/rabbitmq-proxy-forwarder.service
rm -f /etc/logrotate.d/rabbitmq-proxy
rm -f /opt/rabbitmq-proxy/rabbitmq-proxy
rmdir /opt/rabbitmq-proxy 2>/dev/null || true
systemctl daemon-reload

echo "Uninstalled binary, units, and logrotate policy. Configuration and logs were preserved."
