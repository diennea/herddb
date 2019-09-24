#!/bin/bash

set -e

# Allow the container to be started with `--user`
if [[ "$1" = 'bin/service' && "$(id -u)" = '0' ]]; then
    chown -R herddb "$HERD_DATA_DIR" "$HERD_LOG_DIR" "$HERD_CONF_DIR"
    exec gosu herddb "$0" "$@"
fi

exec "$@"
