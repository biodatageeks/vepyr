#!/usr/bin/env bash
# Host firewall for /parity-wgs job containers (run as root; idempotent).
# Containers on the default bridge (docker0) may reach the public internet
# (PyPI) but not the host itself, private/CGNAT ranges or link-local
# (cloud metadata). check.sh verifies these rules.
set -euo pipefail
nets=(10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 169.254.0.0/16 100.64.0.0/10)
iptables -N DOCKER-USER 2>/dev/null || true
for net in "${nets[@]}"; do
  iptables -C DOCKER-USER -i docker0 -d "$net" -j DROP 2>/dev/null \
    || iptables -I DOCKER-USER 1 -i docker0 -d "$net" -j DROP
done
# Container -> host (docker0 address and every other host address) goes
# through INPUT, not DOCKER-USER.
iptables -C INPUT -i docker0 -j DROP 2>/dev/null || iptables -I INPUT 1 -i docker0 -j DROP
echo "egress firewall applied for docker0"
