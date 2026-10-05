#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
if [[ -f .local/runtime.env ]]; then
  source .local/runtime.env
fi
pg_bin="${SHOP_POSTGRES_BIN:?请设置 SHOP_POSTGRES_BIN 为 PostgreSQL bin 目录}"
pg_data="$project_dir/.local/postgres"
pg_run="$project_dir/.local/run"
action="${1:-status}"

case "$action" in
  init)
    mkdir -p "$pg_run"
    if [[ ! -f "$pg_data/PG_VERSION" ]]; then
      "$pg_bin/initdb" -D "$pg_data" -U shop --auth-local=trust --auth-host=trust --locale=C --encoding=UTF8
    fi
    ;;
  start)
    mkdir -p "$pg_run"
    if ! "$pg_bin/pg_isready" -h 127.0.0.1 -p 55432 -U shop -d postgres > /dev/null 2>&1; then
      "$pg_bin/pg_ctl" -D "$pg_data" -l "$pg_data/server.log" -o "-h 127.0.0.1 -p 55432 -k $pg_run" start
    fi
    actual_data="$("$pg_bin/psql" -h 127.0.0.1 -p 55432 -U shop -d postgres -Atc 'SHOW data_directory')"
    if [[ "$actual_data" != "$pg_data" ]]; then
      echo '55432 端口的数据库目录与本项目不同，请检查配置。' >&2
      exit 1
    fi
    for database in shop shop_test; do
      exists="$("$pg_bin/psql" -h 127.0.0.1 -p 55432 -U shop -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = '$database'")"
      if [[ "$exists" != 1 ]]; then
        "$pg_bin/createdb" -h 127.0.0.1 -p 55432 -U shop "$database"
      fi
    done
    "$pg_bin/pg_isready" -h 127.0.0.1 -p 55432 -U shop -d shop
    ;;
  status)
    "$pg_bin/pg_isready" -h 127.0.0.1 -p 55432 -U shop -d shop
    actual_data="$("$pg_bin/psql" -h 127.0.0.1 -p 55432 -U shop -d shop -Atc 'SHOW data_directory')"
    [[ "$actual_data" == "$pg_data" ]]
    echo "数据库目录：$actual_data"
    ;;
  stop)
    "$pg_bin/pg_ctl" -D "$pg_data" -m fast stop
    ;;
  *)
    echo '用法：bash scripts/postgres.sh {init|start|status|stop}' >&2
    exit 2
    ;;
esac
