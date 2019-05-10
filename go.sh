#!/bin/bash
set -xe

redis-server --daemonize yes &
tail -f /dev/null
