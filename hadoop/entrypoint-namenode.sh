#!/usr/bin/env bash
#
# Форматирует NameNode при первом запуске и запускает его.
# Каталог берётся из dfs.namenode.name.dir, чтобы не расходиться с hdfs-site.xml.
#
set -euo pipefail

NAME_DIR_URI="$(hdfs getconf -confKey dfs.namenode.name.dir 2>/dev/null || true)"
NAME_DIR_URI="${NAME_DIR_URI%%,*}"
NAME_DIR="${NAME_DIR_URI#file://}"

if [ -z "$NAME_DIR" ]; then
    NAME_DIR="${NAMENODE_DIR:-/data/data}"
fi

echo "NameNode storage directory: $NAME_DIR"

if [ ! -f "$NAME_DIR/current/VERSION" ]; then
    echo "NameNode is not formatted yet, formatting..."
    mkdir -p "$NAME_DIR" 2>/dev/null || sudo mkdir -p "$NAME_DIR"
    sudo chown -R "$(id -u):$(id -g)" "$NAME_DIR" 2>/dev/null || true
    hdfs namenode -format -force -nonInteractive
else
    echo "NameNode is already formatted."
fi

exec hdfs namenode "$@"
