#!/bin/sh

set -e
set -x

apt-get update
apt-get install -y apt-utils vim less sudo grep util-linux pciutils usbutils coreutils binutils findutils procps net-tools

unzip -q herddb-${project.version}.zip && rm herddb-${project.version}.zip && mv herddb-* /herddb


