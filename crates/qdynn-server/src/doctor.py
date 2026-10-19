"""Optional, dependency-free diagnostics. Input credentials arrive only on stdin."""
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import ssl
import sys
import time


class DiagnosticError(Exception):
    pass


class Probe:
    def __init__(self, config, context=None, port=443):
        self.config = config
        self.host = config['hostname']
        self.context = context or ssl.create_default_context()
        self.port = port

    def connection(self):
        return http.client.HTTPSConnection(self.host, self.port, timeout=5, context=self.context)

    def request(self, method, path, body=None, headers=None, limit=1024 * 1024):
        connection = self.connection()
        try:
            connection.request(method, path, body, {'Host': self.host, **(headers or {})})
            response = connection.getresponse()
            data = response.read(limit + 1)
            if len(data) > limit:
                raise DiagnosticError('response exceeds diagnostic size limit')
            return response.status, response.headers, data
        finally:
            connection.close()

    def bootstrap(self):
        status, headers, data = self.request('GET', '/?bridge=' + self.config['capability'])
        if status != 200:
            raise DiagnosticError('bridge request failed (check frontend routing and capacity)')
        match = re.search(rb'bootstrap="([A-Za-z0-9_-]{43})"', data)
        if not match:
            raise DiagnosticError('bridge unavailable (check hostname, secret and frontend routing)')
        if headers.get('X-Frame-Options') or len(headers.get_all('Content-Security-Policy', [])) != 1:
            raise DiagnosticError('frontend adds incompatible/missing bridge CSP or X-Frame-Options')
        if headers.get('Cache-Control', '').lower() != 'no-store':
            raise DiagnosticError('bridge must use Cache-Control: no-store')
        return match[1].decode()

    def carrier(self, mode):
        bootstrap = self.bootstrap()
        token = None
        try:
            headers = {'Origin': 'https://' + self.host, 'Authorization': 'Bearer ' + bootstrap,
                       'Content-Type': 'application/octet-stream'}
            if self.config['carrier'] == 'auto':
                headers['X-Carrier-Mode'] = mode
            status, response_headers, body = self.request('POST', '/api/v1/session',
                bytes.fromhex('100000000000000101'), headers)
            candidate = response_headers.get('X-Session-Token', '')
            if re.fullmatch(r'[A-Za-z0-9_-]{43}', candidate):
                token = candidate
            if (status != 200 or token is None or body != bytes.fromhex('1100000000000000')
                    or response_headers.get('X-Carrier-Mode') != mode):
                raise DiagnosticError('HELLO/WELCOME failed (check API routing and server carrier)')
            if mode.startswith('websocket'):
                self.websocket(token, mode)
        finally:
            if token:
                try:
                    status, _, _ = self.request('DELETE', '/api/v1/session', headers={
                        'Origin': 'https://' + self.host, 'Authorization': 'Bearer ' + token})
                    if status not in (204, 404, 405):
                        print('WARN: diagnostic session cleanup was not acknowledged', flush=True)
                except Exception:
                    print('WARN: diagnostic session cleanup failed; server TTL will reclaim it', flush=True)

    def websocket(self, token, mode):
        connection = self.connection()
        key = base64.b64encode(os.urandom(16)).decode()
        protocol = ('tproxy-v1.' + token if mode == 'websocket'
                    else 'tproxy-lane-v1.' + token + '.1')
        try:
            connection.request('GET', '/api/v1/ws', headers={
                'Host': self.host, 'Origin': 'https://' + self.host,
                'Upgrade': 'websocket', 'Connection': 'Upgrade',
                'Sec-WebSocket-Version': '13', 'Sec-WebSocket-Key': key,
                'Sec-WebSocket-Protocol': protocol})
            response = connection.getresponse()
            expected = base64.b64encode(hashlib.sha1(
                (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
            if (response.status != 101 or response.getheader('Sec-WebSocket-Accept') != expected
                    or response.getheader('Sec-WebSocket-Protocol') != protocol
                    or response.getheader('Upgrade', '').lower() != 'websocket'
                    or 'upgrade' not in response.getheader('Connection', '').lower().split(', ')):
                raise DiagnosticError('HTTPS works but WebSocket Upgrade failed; check Nginx Upgrade/Connection headers')
        finally:
            connection.close()


def explain(error):
    # Never include exception text: HTTP errors can contain tokens or URLs.
    if isinstance(error, DiagnosticError):
        return str(error)
    if isinstance(error, ssl.SSLCertVerificationError):
        return 'certificate verification failed (name, expiry or trust chain)'
    if isinstance(error, socket.gaierror):
        return 'DNS resolution failed'
    if isinstance(error, (TimeoutError, socket.timeout)):
        return 'network timeout'
    if isinstance(error, ConnectionRefusedError):
        return 'connection refused'
    if isinstance(error, ssl.SSLError):
        return 'TLS handshake failed'
    return 'connection/protocol check failed'


def run(config, probe=None):
    print('OK: configuration is valid', flush=True)
    if not config.get('public_dir'):
        print('WARN: built-in public page; configure your own public_dir for deployment', flush=True)
    elif not (Path(config['public_dir']) / 'index.html').is_file():
        print('FAIL: public_dir has no index.html', flush=True)
        return 1
    probe = probe or Probe(config)
    failures = 0
    def check(name, callback):
        nonlocal failures
        started = time.monotonic()
        try:
            callback()
            print('OK: %s (%d ms)' % (name, (time.monotonic() - started) * 1000), flush=True)
        except Exception as error:
            failures += 1
            print('FAIL: %s: %s' % (name, explain(error)), flush=True)

    def backend():
        with socket.create_connection((config['backend_host'], config['backend_port']), timeout=3):
            pass
    check('local backend TCP', backend)
    def public():
        status, _, _ = probe.request('GET', '/')
        if status != 200:
            raise DiagnosticError('public site does not return HTTP 200')
    check('public DNS / TLS / HTTPS', public)
    modes = ['https', 'websocket'] if config['carrier'] == 'auto' else [config['carrier']]
    for mode in modes:
        check(mode + ' HELLO/WELCOME' + (' / Upgrade' if mode.startswith('websocket') else ''),
              lambda mode=mode: probe.carrier(mode))
    print('Checks cover this server/network path, not Android/WebView or MTProto handshake.', flush=True)
    return int(failures != 0)


if __name__ == '__main__':
    try:
        sys.exit(run(json.load(sys.stdin)))
    except Exception as error:
        print('FAIL: doctor: ' + explain(error), flush=True)
        sys.exit(1)
