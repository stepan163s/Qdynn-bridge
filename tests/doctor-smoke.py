"""Real relay + local certificate-verified TLS frontend; no external requests."""
import contextlib
import hashlib
import hmac
import base64
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('doctor', root / 'crates/qdynn-server/src/doctor.py')
doctor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(doctor)
binary = Path(sys.argv[1] if len(sys.argv) > 1 else root / 'target/debug/qdynn-bridge').resolve()
secret = '000102030405060708090a0b0c0d0e0f'
capability = base64.urlsafe_b64encode(hmac.new(bytes.fromhex(secret), b'tdesktop-web-proxy-bridge-v1\nlocalhost', hashlib.sha256).digest()).decode().rstrip('=')

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

with tempfile.TemporaryDirectory() as directory:
    directory = Path(directory)
    (directory / 'openssl.cnf').write_text('[req]\ndistinguished_name=dn\nx509_extensions=v3\nprompt=no\n[dn]\nCN=localhost\n[v3]\nsubjectAltName=DNS:localhost\nbasicConstraints=critical,CA:TRUE\n')
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
        '-config', str(directory / 'openssl.cnf'), '-keyout', str(directory / 'key.pem'),
        '-out', str(directory / 'cert.pem')], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    backend = socket.socket(); backend.bind(('127.0.0.1', 0)); backend.listen()
    def accept():
        try:
            while True:
                connection, _ = backend.accept(); connection.close()
        except OSError:
            pass
    threading.Thread(target=accept, daemon=True).start()
    relay_port = port()
    process = subprocess.Popen([str(binary), '--hostname', 'localhost', '--listen', '127.0.0.1:' + str(relay_port),
        '--backend', '127.0.0.1:' + str(backend.getsockname()[1]), '--carrier-mode', 'auto'],
        env={**os.environ, 'QDYNN_SECRET': secret}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tls = None
    try:
        for _ in range(200):
            try:
                with socket.create_connection(('127.0.0.1', relay_port), timeout=.1): break
            except OSError: time.sleep(.025)
        else: raise AssertionError('relay did not start')
        class Frontend(BaseHTTPRequestHandler):
            broken_ws = False
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_): pass
            def relay(self):
                if self.broken_ws and self.path == '/api/v1/ws':
                    self.send_response(400); self.send_header('Content-Length', '0'); self.end_headers(); return
                connection = http.client.HTTPConnection('127.0.0.1', relay_port, timeout=5)
                try:
                    body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                    connection.request(self.command, self.path, body, dict(self.headers))
                    response = connection.getresponse()
                    self.send_response_only(response.status)
                    for name, value in response.getheaders(): self.send_header(name, value)
                    self.end_headers()
                    if response.status != 101: self.wfile.write(response.read())
                    else: self.close_connection = True
                finally: connection.close()
            do_GET = do_POST = do_DELETE = relay
        tls = ThreadingHTTPServer(('127.0.0.1', 0), Frontend)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(directory / 'cert.pem', directory / 'key.pem')
        tls.socket = context.wrap_socket(tls.socket, server_side=True)
        threading.Thread(target=tls.serve_forever, daemon=True).start()
        trusted = ssl.create_default_context(cafile=str(directory / 'cert.pem'))
        config = dict(hostname='localhost', capability=capability, backend_host='127.0.0.1',
            backend_port=backend.getsockname()[1], carrier='auto', public_dir=None)
        probe = doctor.Probe(config, trusted, tls.server_port)
        output = io.StringIO()
        with contextlib.redirect_stdout(output): assert doctor.run(config, probe) == 0
        assert 'websocket HELLO/WELCOME / Upgrade' in output.getvalue()
        for value in (secret, capability, 'tproxy-v1.'): assert value not in output.getvalue()
        Frontend.broken_ws = True
        output = io.StringIO()
        with contextlib.redirect_stdout(output): assert doctor.run(config, probe) == 1
        assert 'HTTPS works but WebSocket Upgrade failed' in output.getvalue()
        Frontend.broken_ws = False
        untrusted = doctor.Probe(config, port=tls.server_port)
        try: untrusted.request('GET', '/')
        except ssl.SSLCertVerificationError: pass
        else: raise AssertionError('untrusted certificate accepted')
        bad = doctor.Probe({**config, 'capability': 'x' * 43}, trusted, tls.server_port)
        try: bad.carrier('https')
        except doctor.DiagnosticError as error: assert 'bridge unavailable' in str(error)
        else: raise AssertionError('bad capability accepted')
        assert secret not in doctor.explain(ValueError(secret))
        print('PASS: doctor against real relay/TLS, HTTPS and WS, blocked Upgrade, certificate rejection, bad capability, redaction')
    finally:
        process.terminate()
        try: process.wait(timeout=5)
        except subprocess.TimeoutExpired: process.kill(); process.wait()
        if tls: tls.shutdown(); tls.server_close()
        backend.close()
