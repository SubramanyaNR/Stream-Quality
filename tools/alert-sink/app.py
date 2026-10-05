"""Demo Alertmanager webhook receiver: prints each alert as one JSON log line (Alloy -> Loki)."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        for a in body.get("alerts", []):
            print(json.dumps({"event": "alert", "status": a.get("status"), "labels": a.get("labels"),
                              "summary": a.get("annotations", {}).get("summary")}), flush=True)
        self.send_response(200); self.end_headers()
    def log_message(self, *a): pass

HTTPServer(("0.0.0.0", 5001), H).serve_forever()
