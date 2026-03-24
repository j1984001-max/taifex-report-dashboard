#!/usr/bin/env python3
import argparse
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def parse_agents(value):
    agents = []
    for part in value.split(','):
        p = part.strip()
        if not p:
            continue
        if p.startswith("openclaw:"):
            p = p.split(":", 1)[1]
        if p.startswith("agent:"):
            p = p.split(":", 1)[1]
        if p not in agents:
            agents.append(p)
    return agents


class ProxyHandler(BaseHTTPRequestHandler):
    server_version = "OpenClawAutoSwitch/0.1"

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_error(404, "Not Found")
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)

        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            self.send_error(400, "Invalid JSON")
            return

        if payload.get("stream") is True:
            self.send_error(400, "stream=true is not supported by this proxy")
            return

        request_agents = []
        model = payload.get("model")
        if isinstance(model, str) and (model.startswith("openclaw:") or model.startswith("agent:")):
            request_agents = parse_agents(model)

        agents = []
        for a in request_agents + self.server.agents:
            if a not in agents:
                agents.append(a)

        if not agents:
            self.send_error(400, "No agents configured")
            return

        auth_header = self.headers.get("Authorization")

        last_error = None
        for agent_id in agents:
            payload["model"] = f"openclaw:{agent_id}"
            try:
                status, headers, resp_body = self.forward(payload, auth_header)
            except Exception as e:
                last_error = ("proxy_exception", str(e))
                if self.server.debug:
                    print(f"[proxy] {agent_id} exception: {e}", file=sys.stderr)
                continue

            if status == 200:
                self.send_response(200)
                self.send_header("Content-Type", headers.get("Content-Type", "application/json"))
                self.end_headers()
                self.wfile.write(resp_body)
                return

            # Retry on transient failures
            if status in (408, 429, 500, 502, 503, 504):
                last_error = ("upstream_error", f"status {status}")
                if self.server.debug:
                    print(f"[proxy] {agent_id} transient status {status}", file=sys.stderr)
                continue

            # Hard failure, return immediately
            self.send_response(status)
            self.send_header("Content-Type", headers.get("Content-Type", "application/json"))
            self.end_headers()
            self.wfile.write(resp_body)
            return

        # All fallbacks failed
        msg = "All fallback agents failed"
        if last_error:
            msg = f"{msg}: {last_error[0]} ({last_error[1]})"
        self.send_response(502)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": {"message": msg, "type": "proxy_error"}}).encode("utf-8"))

    def log_message(self, fmt, *args):
        if self.server.debug:
            super().log_message(fmt, *args)

    def forward(self, payload, auth_header):
        data = json.dumps(payload).encode("utf-8")
        req = Request(self.server.gateway_url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        if auth_header:
            req.add_header("Authorization", auth_header)

        try:
            with urlopen(req, timeout=self.server.timeout) as resp:
                return resp.getcode(), resp.headers, resp.read()
        except HTTPError as e:
            return e.code, e.headers, e.read()
        except URLError as e:
            raise RuntimeError(e.reason)


def main():
    parser = argparse.ArgumentParser(description="OpenClaw Chat Completions proxy with agent failover")
    parser.add_argument("--gateway", default="http://127.0.0.1:18789/v1/chat/completions")
    parser.add_argument("--listen", default="127.0.0.1:18790")
    parser.add_argument("--agents", default="main")
    parser.add_argument("--timeout", type=float, default=25.0)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    agents = parse_agents(args.agents)
    if not agents:
        print("No agents configured", file=sys.stderr)
        sys.exit(2)

    host, port = args.listen.split(":", 1)
    port = int(port)

    server = HTTPServer((host, port), ProxyHandler)
    server.gateway_url = args.gateway
    server.agents = agents
    server.timeout = args.timeout
    server.debug = args.debug

    print(f"OpenClaw autoswitch proxy listening on {host}:{port}")
    print(f"Gateway: {server.gateway_url}")
    print(f"Agents: {', '.join(agents)}")
    sys.stdout.flush()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
