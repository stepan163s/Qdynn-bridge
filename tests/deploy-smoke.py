"""Offline staged installation/update/rollback using the actual executable."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parents[1]
binary = Path(sys.argv[1] if len(sys.argv) > 1 else root / 'target/debug/qdynn-bridge').resolve()
with tempfile.TemporaryDirectory() as temporary:
    temporary = Path(temporary)
    config = temporary / 'source.toml'
    config.write_text("hostname='localhost'\nsecret='000102030405060708090a0b0c0d0e0f'\n")
    stage = temporary / 'stage'
    environment = {**os.environ, 'QDYNN_ROOT': str(stage), 'QDYNN_SECRET': 'invalid-inherited-override'}
    def run(*args, success=True):
        result = subprocess.run(['bash', str(root / 'deploy/manage.sh'), *map(str, args)],
            env=environment, capture_output=True, timeout=15)
        assert (result.returncode == 0) == success, result.stderr.decode()
    def digest(path): return hashlib.sha256(path.read_bytes()).digest()
    run('install', binary, config)
    installed = stage / 'usr/local/bin/qdynn-bridge'
    installed_config = stage / 'etc/qdynn/config.toml'
    assert digest(installed) == digest(binary)
    assert installed_config.read_bytes() == config.read_bytes()
    assert installed_config.stat().st_mode & 0o777 == 0o640
    run('install', binary, config, success=False)
    # A distinct executable revision that still validates the same configuration.
    candidate = temporary / 'candidate'
    candidate.write_text('#!/bin/sh\nexec "' + str(binary) + '" "$@"\n')
    candidate.chmod(0o755)
    run('update', candidate)
    assert digest(installed) == digest(candidate)
    assert digest(installed.with_name('qdynn-bridge.previous')) == digest(binary)
    bad = temporary / 'bad'; bad.write_text('#!/bin/sh\nexit 42\n'); bad.chmod(0o755)
    run('update', bad, success=False)
    assert digest(installed) == digest(candidate)
    run('rollback')
    assert digest(installed) == digest(binary)
    assert installed_config.read_bytes() == config.read_bytes()
    lock = installed.parent / '.qdynn-deploy-lock'
    lock.mkdir()
    run('update', candidate, success=False)
    assert lock.exists()
    lock.rmdir()
    assert not installed.with_name('.qdynn-candidate').exists()
print('PASS: offline install/update/rollback, failed candidate, config preservation, override isolation, lock ownership')
