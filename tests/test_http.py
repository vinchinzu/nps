from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from i990.http import download_resumable


PAYLOAD = b"abcdefghijklmnopqrstuvwxyz" * 64


class _NoRangeHandler(BaseHTTPRequestHandler):
    def do_HEAD(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Length", str(len(PAYLOAD)))
        self.end_headers()
        self.wfile.write(PAYLOAD)

    def log_message(self, format: str, *args: object) -> None:
        return


class DownloadResumableTest(unittest.TestCase):
    def test_restarts_when_server_ignores_range(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _NoRangeHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as td:
                dest = Path(td) / "sample.zip"
                part = dest.with_suffix(".zip.part")
                part.write_bytes(PAYLOAD[:100])
                download_resumable(f"http://127.0.0.1:{server.server_port}/file.zip", dest)
                self.assertEqual(dest.read_bytes(), PAYLOAD)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
