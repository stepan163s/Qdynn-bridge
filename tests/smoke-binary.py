"""Smoke test of the actual binary: loopback only, synthetic credentials."""
import base64
import hashlib
import hmac
import http.client
import os
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

binary = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / 'target/release/qdynn-bridge'
secret = '000102030405060708090a0b0c0d0e0f'
host = 'smoke.example'


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def request(port, method, path, body=None, headers=None):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    conn.request(method, path, body, {'Host': host, **(headers or {})})
    response = conn.getresponse()
    result = response.status, dict(response.getheaders()), response.read()
    conn.close()
    return result


backend = socket.socket()
backend.bind(('127.0.0.1', 0))
backend.listen()
active = set()
lock = threading.Lock()


def consume(conn):
    with lock:
        active.add(conn)
    try:
        while conn.recv(65536):
            pass
    finally:
        conn.close()
        with lock:
            active.discard(conn)


def accept():
    try:
        while True:
            conn, _ = backend.accept()
            threading.Thread(target=consume, args=(conn,), daemon=True).start()
    except OSError:
        pass


threading.Thread(target=accept, daemon=True).start()
http_port, admin_port = free_port(), free_port()
command = [str(binary), '--hostname', host, '--secret', secret,
           '--listen', f'127.0.0.1:{http_port}', '--admin-listen', f'127.0.0.1:{admin_port}',
           '--backend', f'127.0.0.1:{backend.getsockname()[1]}']
proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        env={**os.environ, 'RUST_LOG': 'debug'})
ws = None
try:
    deadline = time.monotonic() + 5
    while True:
        try:
            assert request(admin_port, 'GET', '/healthz')[0] == 200
            break
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.025)
    assert request(admin_port, 'GET', '/readyz')[0] == 204
    capability = base64.urlsafe_b64encode(hmac.new(bytes.fromhex(secret),
        ('tdesktop-web-proxy-bridge-v1\n' + host).encode(), hashlib.sha256).digest()).decode().rstrip('=')
    status, _, page = request(http_port, 'GET', '/?bridge=' + capability)
    assert status == 200
    bootstrap = re.search(rb'bootstrap="([A-Za-z0-9_-]+)"', page)[1].decode()
    hello = bytes.fromhex('100000000000000101')
    headers = {'Authorization': 'Bearer ' + bootstrap, 'Content-Type': 'application/octet-stream'}
    first = request(http_port, 'POST', '/api/v1/session', hello, headers)
    retry = request(http_port, 'POST', '/api/v1/session', hello, headers)
    assert first[0] == retry[0] == 200
    assert first[1]['x-session-token'] == retry[1]['x-session-token']
    assert first[2] == retry[2] == bytes.fromhex('1100000000000000')
    token = first[1]['x-session-token']
    ws = socket.create_connection(('127.0.0.1', http_port), timeout=2)
    key = base64.b64encode(os.urandom(16)).decode()
    ws.sendall((f'GET /api/v1/ws HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                f'Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\nOrigin: https://{host}\r\n'
                f'Sec-WebSocket-Protocol: tproxy-v1.{token}\r\n\r\n').encode())
    handshake = b''
    while b'\r\n\r\n' not in handshake:
        handshake += ws.recv(4096)
    assert handshake.startswith(b'HTTP/1.1 101 ')
    payload, mask = bytes.fromhex('0100000100000000'), os.urandom(4)
    ws.sendall(struct.pack('BB', 0x82, 0x80 | len(payload)) + mask +
               bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload)))
    deadline = time.monotonic() + 2
    while True:
        metrics = request(admin_port, 'GET', '/metrics')[2]
        with lock:
            connected = bool(active)
        if b'qdynn_streams 1\n' in metrics and connected:
            break
        assert time.monotonic() < deadline, 'WS OPEN did not reach TCP backend'
        time.sleep(0.01)
    proc.send_signal(signal.SIGTERM)
    output = proc.communicate(timeout=5)[0].decode()
    assert proc.returncode == 0, output
    deadline = time.monotonic() + 1
    while True:
        with lock:
            if not active:
                break
        assert time.monotonic() < deadline, 'backend socket survived process shutdown'
        time.sleep(0.01)
    assert all(value not in output for value in (secret, bootstrap, token)), 'secret or bearer found in logs'
    help_result = subprocess.run([str(binary), '--help'], capture_output=True, timeout=5)
    assert help_result.returncode == 0
    links = subprocess.run([str(binary), '--hostname', host, '--secret', secret, '--print-link'], capture_output=True, timeout=5)
    assert links.returncode == 0 and b'tg://webproxy?' in links.stdout
    print('PASS: binary startup, bootstrap replay, WS/TCP, private metrics, SIGTERM, redacted logs, CLI')
finally:
    if proc.poll() is None:
        proc.kill()
        proc.communicate()
    if ws:
        ws.close()
    backend.close()
