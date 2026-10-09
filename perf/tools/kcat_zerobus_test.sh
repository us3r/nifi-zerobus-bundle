#!/usr/bin/env bash
# Checks whether a stock librdkafka client (kcat) can produce to Zerobus through its
# Kafka-compatible API, using librdkafka's built-in OIDC client-credentials flow.
# That is the same code path rsyslog's omkafka would use.
#
# Runs kcat from Debian in a throwaway container: the common kcat images ship a
# librdkafka without OIDC support.
#
# Required:
#   ZEROBUS_ENDPOINT        <workspace-id>.zerobus.<region>.cloud.databricks.com
#   DATABRICKS_WORKSPACE    https://<workspace>.cloud.databricks.com
#   ZEROBUS_TABLE           catalog.schema.table (zerobus_perf layout, see perf/README.md)
#   DATABRICKS_CLIENT_ID    service principal client ID
#   ZEROBUS_CLIENT_SECRET   its secret, or ZEROBUS_CLIENT_SECRET_FILE pointing at a file with it
set -euo pipefail

if [[ -z "${ZEROBUS_CLIENT_SECRET:-}" && -n "${ZEROBUS_CLIENT_SECRET_FILE:-}" ]]; then
    ZEROBUS_CLIENT_SECRET=$(tr -d '\r\n' < "$ZEROBUS_CLIENT_SECRET_FILE")
fi
for v in ZEROBUS_ENDPOINT DATABRICKS_WORKSPACE ZEROBUS_TABLE DATABRICKS_CLIENT_ID ZEROBUS_CLIENT_SECRET; do
    [[ -n "${!v:-}" ]] || { echo "$v is not set" >&2; exit 2; }
done

ZEROBUS_ENDPOINT=${ZEROBUS_ENDPOINT#https://}
export ZEROBUS_ENDPOINT DATABRICKS_WORKSPACE ZEROBUS_TABLE DATABRICKS_CLIENT_ID ZEROBUS_CLIENT_SECRET

docker run --rm -i \
    -e ZEROBUS_ENDPOINT -e DATABRICKS_WORKSPACE -e ZEROBUS_TABLE -e DATABRICKS_CLIENT_ID -e ZEROBUS_CLIENT_SECRET \
    debian:trixie-slim bash -s <<'IN_CONTAINER'
set -uo pipefail
apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq kcat ca-certificates >/dev/null 2>&1
kcat -V 2>&1 | grep -i version

umask 077
cat > /tmp/kcat.conf <<CONF
bootstrap.servers=${ZEROBUS_ENDPOINT}:9092
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.oauthbearer.method=oidc
sasl.oauthbearer.token.endpoint.url=${DATABRICKS_WORKSPACE%/}/oidc/v1/token
sasl.oauthbearer.client.id=${DATABRICKS_CLIENT_ID}
sasl.oauthbearer.client.secret=${ZEROBUS_CLIENT_SECRET}
sasl.oauthbearer.scope=all-apis
compression.codec=none
request.required.acks=-1
CONF

echo "--- 1. metadata for topic ${ZEROBUS_TABLE}"
kcat -F /tmp/kcat.conf -L -t "${ZEROBUS_TABLE}" -m 20 2>&1 | tail -15
echo "    exit code: $?"

echo "--- 2. produce one JSON record"
record="{\"event_ts\":$(date +%s%3N),\"source\":\"kafka-api\",\"asset_id\":\"kcat\",\"event_type\":\"connectivity_test\",\"severity\":\"low\",\"payload\":\"sent by kcat through the Zerobus Kafka-compatible API\"}"
echo "$record" | kcat -F /tmp/kcat.conf -P -t "${ZEROBUS_TABLE}" -X message.timeout.ms=30000 -d security 2>&1 | tail -25
echo "    exit code: ${PIPESTATUS[1]}"
echo "If both steps succeeded: SELECT * FROM ${ZEROBUS_TABLE} WHERE source = 'kafka-api';"
IN_CONTAINER
