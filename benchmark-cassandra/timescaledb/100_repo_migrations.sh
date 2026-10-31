#!/bin/bash
# Applies the repo's Flyway migrations (V0..V5, mounted read-only at /repo-migrations) in version order, after the
# image's own init scripts (000_install_timescaledb.sh, 001_timescaledb_tune.sh) have run.
set -euo pipefail
for file in $(ls /repo-migrations/V*__*.sql | sort -V); do
    echo "applying ${file}"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$file"
done
