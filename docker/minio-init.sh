#!/bin/sh
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
for bucket in lifecycle-staging lifecycle-dev lifecycle-archive; do
    mc mb --ignore-existing "local/$bucket"
done
mc admin user add local "$MINIO_APP_USER" "$MINIO_APP_PASSWORD" >/dev/null
mc admin policy create local lifecycle-app /bootstrap/policy.json
mc admin policy attach local lifecycle-app --user "$MINIO_APP_USER"