"""Exercise delivery proofs over actual TLS, including interrupted transfers."""

import json
import shutil
import ssl
import subprocess
import threading
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from chatstore.archive import delivery
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from tests.unit.test_cache import msg


def test_https_delivery_retrieves_both_objects_and_retries_truncated_transfer(tmp_path, monkeypatch):
    openssl = shutil.which('openssl')
    if openssl is None:
        pytest.skip('TLS fixture requires openssl')
    cert, key = tmp_path / 'cert.pem', tmp_path / 'key.pem'
    subprocess.run([
        openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost',
        '-keyout', str(key), '-out', str(cert),
    ], check=True, capture_output=True)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    client_context = ssl.create_default_context(cafile=str(cert))
    monkeypatch.setattr(delivery.ssl, 'create_default_context', lambda: client_context)
    objects, requests = {}, []
    truncate = False

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.path, self.headers.get('Cache-Control')))
            payload = objects[self.path]
            self.send_response(200)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload[:len(payload) // 2] if truncate and self.path == '/archive' else payload)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dd = DataDir(tmp_path / 'data')
        dd.create()
        with closing(Cache(dd.cache)) as cache:
            with cache.write():
                cache.upsert(msg(1, 'preserved across remote delivery', None, None, 1_000_000), observed_at=100)
            batch = export_changes(cache, dd, Config(), Identity(), consumer='remote',
                                   output=dd.exports, password='test-password', media='text')
            objects['/receipt'] = json.dumps(batch['receipt']).encode()
            objects['/archive'] = Path(batch['path']).read_bytes()
            base = f'https://localhost:{server.server_port}'

            def confirm():
                return delivery.confirm(cache, destination='linux', export_id=batch['export_id'],
                                        receipt_url=base + '/receipt', archive_url=base + '/archive')

            truncate = True
            with pytest.raises(ValueError, match='ciphertext does not match'):
                confirm()
            assert cache.conn.execute('SELECT count(*) FROM delivery_checkpoints').fetchone()[0] == 0
            assert len(delivery.pending(cache, 'remote', 'linux')) == 1
            truncate = False
            proof = confirm()
            assert proof['ciphertext_bytes'] == len(objects['/archive'])
            assert delivery.pending(cache, 'remote', 'linux') == []
            confirm()
            assert [path for path, _ in requests] == ['/receipt', '/archive'] * 3
            assert all(policy == 'no-cache, no-store' for _, policy in requests)
            assert cache.conn.execute('SELECT count(*) FROM delivery_proofs').fetchone()[0] == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
