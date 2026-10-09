# Sending to Zerobus Ingest from librdkafka clients (syslog-ng, rsyslog, kcat)

Zerobus Ingest exposes a Kafka-compatible API, so any producer built on librdkafka can
write to a Delta table. One thing stands in the way: Zerobus only accepts OAuth tokens
that are scoped to the target table, and librdkafka's built-in OIDC flow cannot request
such a token. This guide works around that with a small local token proxy, so the client
itself needs configuration only.

## How it works

```
syslog-ng / rsyslog / kcat           token proxy (localhost)          Databricks
        |                                    |                            |
        |-- POST /token (client creds) ----->|                            |
        |                                    |-- same request, plus ----->|
        |                                    |   resource +               |
        |                                    |   authorization_details    |
        |<------- table-scoped token --------|<---------------------------|
        |
        |-- SASL_SSL / OAUTHBEARER, produce ---------------------------> Zerobus :9092
```

librdkafka sends a plain client-credentials request. The proxy adds the two fields that
scope the token to one table, forwards the request to the workspace token endpoint and
returns the response untouched. The client ID and secret come from the client; the proxy
stores no credentials.

Without the proxy the broker answers `SASL authentication error: Invalid token.`
Appending the extra fields to the token URL as query parameters does not help: Databricks
reads them from the request body only.

## Prerequisites

- The **Zerobus Ingest Kafka endpoint** preview is enabled for the workspace (workspace
  admin, **Previews** page). Otherwise authentication fails with
  `feature "Zerobus Ingest Kafka Endpoint" is not enabled for workspace`.
- A service principal with an OAuth secret and these privileges: `USE CATALOG`,
  `USE SCHEMA`, and `SELECT` + `MODIFY` on the target table.
- The target table exists. Zerobus does not create tables.
- librdkafka built with OIDC support. Check that `builtin.features` lists `http,oidc`
  (for example with `kcat -V` or the client's debug log). Version 1.8.x and builds
  without libcurl do not have it.
- Python 3.8+ on the host that runs the client. The proxy has no dependencies.

Values used below:

| Placeholder | Example |
|---|---|
| `<workspace-url>` | `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com` |
| `<workspace-id>` | numeric workspace ID |
| `<region>` | `eu-west-1` |
| `<zerobus-host>` | `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| `<table>` | `catalog.schema.table` |

## 1. Create the table

```sql
CREATE TABLE main.logs.syslog_events (
  event_ts BIGINT,      -- epoch seconds
  host     STRING,
  program  STRING,
  pid      STRING,
  facility STRING,
  severity STRING,
  message  STRING
) USING DELTA;

GRANT USE CATALOG ON CATALOG main TO `<client-id>`;
GRANT USE SCHEMA ON SCHEMA main.logs TO `<client-id>`;
GRANT SELECT, MODIFY ON TABLE main.logs.syslog_events TO `<client-id>`;
```

Each Kafka message must be one JSON object whose keys match the column names.

## 2. Install the token proxy

Save as `/opt/zerobus/zerobus_token_proxy.py`:

```python
"""Local OAuth token proxy for librdkafka clients writing to Zerobus.

librdkafka's built-in OIDC flow cannot add `resource` and `authorization_details`
to the token request, and Zerobus rejects tokens issued without them. This proxy
accepts librdkafka's plain client-credentials request, adds the two fields for the
configured table and forwards it to the Databricks token endpoint unchanged otherwise.
"""
import json, os, urllib.error, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WORKSPACE = os.environ["DATABRICKS_WORKSPACE"].rstrip("/")
WORKSPACE_ID = os.environ["ZEROBUS_ENDPOINT"].replace("https://", "").split(".")[0]
TABLE = os.environ["ZEROBUS_TABLE"]
catalog, schema, _ = TABLE.split(".")
EXTRA = {
    "resource": f"api://databricks/workspaces/{WORKSPACE_ID}/zerobusDirectWriteApi",
    "authorization_details": json.dumps([
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"], "object_type": "CATALOG", "object_full_path": catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"], "object_type": "SCHEMA", "object_full_path": f"{catalog}.{schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"], "object_type": "TABLE", "object_full_path": TABLE},
    ]),
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        form = dict(urllib.parse.parse_qsl(self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()))
        form.update(EXTRA)
        req = urllib.request.Request(WORKSPACE + "/oidc/v1/token", data=urllib.parse.urlencode(form).encode(), method="POST")
        for h in ("Authorization", "Content-Type"):
            if self.headers.get(h):
                req.add_header(h, self.headers[h])
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                status, body = r.status, r.read()
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print("token-proxy:", fmt % args, flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("PROXY_PORT", "8181"))), Handler).serve_forever()
```

The proxy listens on `127.0.0.1` only. Keep it that way: it forwards credentials over
plain HTTP, which is acceptable on loopback and nowhere else.

### Run it as a service

`/etc/systemd/system/zerobus-token-proxy.service`:

```ini
[Unit]
Description=Zerobus OAuth token proxy for librdkafka clients
After=network-online.target
Wants=network-online.target

[Service]
Environment=DATABRICKS_WORKSPACE=<workspace-url>
Environment=ZEROBUS_ENDPOINT=<zerobus-host>
Environment=ZEROBUS_TABLE=main.logs.syslog_events
Environment=PROXY_PORT=8181
ExecStart=/usr/bin/python3 /opt/zerobus/zerobus_token_proxy.py
Restart=always
DynamicUser=yes

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now zerobus-token-proxy
```

## 3. Verify with kcat

`zerobus.conf`:

```properties
bootstrap.servers=<zerobus-host>:9092
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.oauthbearer.method=oidc
sasl.oauthbearer.token.endpoint.url=http://127.0.0.1:8181/token
sasl.oauthbearer.client.id=<client-id>
sasl.oauthbearer.client.secret=<client-secret>
sasl.oauthbearer.scope=all-apis
compression.codec=none
request.required.acks=-1
```

```bash
chmod 600 zerobus.conf

# metadata: expect one broker and the topic with one partition
kcat -F zerobus.conf -L -t main.logs.syslog_events

# produce one record
echo '{"event_ts":1760000000,"host":"test","program":"kcat","message":"hello"}' \
  | kcat -F zerobus.conf -P -t main.logs.syslog_events
```

The topic name is the full table name. Data becomes visible in the table within a few
seconds.

## 4. Configure the client

### syslog-ng

```
destination d_zerobus {
  kafka(
    bootstrap-servers("<zerobus-host>:9092")
    topic("main.logs.syslog_events")
    message('$(format-json --scope none
               event_ts=int64(${R_UNIXTIME})
               host=${HOST}
               program=${PROGRAM}
               pid=${PID}
               facility=${FACILITY}
               severity=${LEVEL}
               message=${MESSAGE})')
    config(
      "security.protocol"("SASL_SSL")
      "sasl.mechanism"("OAUTHBEARER")
      "sasl.oauthbearer.method"("oidc")
      "sasl.oauthbearer.token.endpoint.url"("http://127.0.0.1:8181/token")
      "sasl.oauthbearer.client.id"("<client-id>")
      "sasl.oauthbearer.client.secret"("`ZEROBUS_CLIENT_SECRET`")
      "sasl.oauthbearer.scope"("all-apis")
      "compression.codec"("none")
      "request.required.acks"("-1")
    )
    workers(4)
    disk-buffer(reliable(yes) mem-buf-size(64MiB) disk-buf-size(4GiB))
  );
};

log { source(s_src); destination(d_zerobus); };
```

The backticks read the secret from the environment of the syslog-ng process, which keeps
it out of the configuration file.

### rsyslog (omkafka)

```
module(load="omkafka")

template(name="zerobus_json" type="list" option.jsonf="on") {
  property(outname="event_ts" name="timereported" dateFormat="unixtimestamp" format="jsonf" datatype="number")
  property(outname="host"     name="hostname"                format="jsonf")
  property(outname="program"  name="programname"             format="jsonf")
  property(outname="pid"      name="procid"                  format="jsonf")
  property(outname="facility" name="syslogfacility-text"     format="jsonf")
  property(outname="severity" name="syslogseverity-text"     format="jsonf")
  property(outname="message"  name="msg"                     format="jsonf")
}

action(type="omkafka"
       broker=["<zerobus-host>:9092"]
       topic="main.logs.syslog_events"
       template="zerobus_json"
       confParam=["security.protocol=SASL_SSL",
                  "sasl.mechanism=OAUTHBEARER",
                  "sasl.oauthbearer.method=oidc",
                  "sasl.oauthbearer.token.endpoint.url=http://127.0.0.1:8181/token",
                  "sasl.oauthbearer.client.id=<client-id>",
                  "sasl.oauthbearer.client.secret=<client-secret>",
                  "sasl.oauthbearer.scope=all-apis",
                  "compression.codec=none",
                  "request.required.acks=-1"])
```

## Limits and caveats

- **JSON only, no compression.** The Kafka-compatible API takes JSON values and requires
  `compression.codec=none`.
- **Write-only.** Consumer, admin and transactional APIs are not available.
- **Throughput quota.** The Kafka-compatible API has its own quota, lower than the gRPC
  SDKs. For high volumes use a Zerobus SDK instead.
- **At-least-once.** Retries can produce duplicates; deduplicate downstream if that
  matters.
- **One table per proxy.** The token is scoped to `ZEROBUS_TABLE`. For several tables run
  one proxy per table on different ports, or extend `authorization_details` in the proxy.
- **The proxy is on the critical path for token refresh.** Tokens last one hour. If the
  proxy is down when librdkafka refreshes, producing stops until it is back.

## Troubleshooting

| Error | Cause |
|---|---|
| `SASL authentication error: Invalid token.` | The client is talking to the Databricks token endpoint directly, not to the proxy. |
| `feature "Zerobus Ingest Kafka Endpoint" is not enabled for workspace` | The preview is not enabled for the workspace. |
| `Unsupported value "oidc" for sasl.oauthbearer.method` or `OAUTHBEARER` not built in | librdkafka lacks OIDC support; use a newer build with libcurl. |
| Token request returns 401 through the proxy | Wrong client ID or secret, or the service principal lacks privileges on the table. |

## What was verified

kcat on Debian trixie (librdkafka 2.8.0) fetched metadata and produced a record to a
Zerobus table in AWS `eu-west-1` using the configuration in step 3 and the proxy above.
The syslog-ng and rsyslog snippets pass the same librdkafka properties but were not run
end to end.
