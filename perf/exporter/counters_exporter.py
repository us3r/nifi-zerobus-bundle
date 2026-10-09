"""Exposes NiFi processor counters as real Prometheus counters.

NiFi's own /flow/metrics/prometheus endpoint reports processor counters as a rolling
5-minute sum, which makes rate() useless. The REST API has the cumulative values, so
this polls it on every scrape and re-exports them.
"""
import json
import os
import re
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

NIFI_API = os.environ.get("NIFI_API", "http://nifi:8080/nifi-api")
# Per-processor counter contexts look like "Processor Name (uuid)"; the rest are per-type totals
CONTEXT = re.compile(r"^(?P<name>.*) \((?P<id>[0-9a-f-]{36})\)$")


def escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def render():
    with urllib.request.urlopen(NIFI_API + "/counters", timeout=3) as response:
        counters = json.load(response)["counters"]["aggregateSnapshot"]["counters"]
    lines = ["# HELP nifi_counter_total Cumulative value of a NiFi processor counter",
             "# TYPE nifi_counter_total counter"]
    for counter in counters:
        match = CONTEXT.match(counter["context"])
        if match:
            lines.append('nifi_counter_total{processor_name="%s",processor_id="%s",counter_name="%s"} %d' % (
                escape(match["name"]), match["id"], escape(counter["name"]), counter["valueCount"]))
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            body, status = render().encode(), 200
        except Exception as e:  # NiFi still starting, or restarting
            body, status = f"# {e}\n".encode(), 503
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


HTTPServer(("0.0.0.0", 9100), Handler).serve_forever()
