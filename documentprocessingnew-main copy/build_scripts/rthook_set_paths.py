"""
build_scripts/rthook_set_paths.py
──────────────────────────────────
PyInstaller runtime hook – executes BEFORE any application code.

Responsibilities:
  1. Expose DOCPRO_BASE_PATH so every module can find bundled assets.
  2. Point requests/urllib3 at the bundled CA certificate bundle.
  3. Keep stdout/stderr writable even when console=False.
  4. Set a sensible working directory (beside the EXE, not %TEMP%\_MEI...).
"""

import os
import sys

# ── 1.  Resolve the real base path ────────────────────────────────────────────
#
#   Frozen (PyInstaller onefile)  →  sys._MEIPASS  (temp extraction dir)
#   Frozen (PyInstaller one-dir)  →  sys._MEIPASS  (same, but stays on disk)
#   Plain Python                  →  directory of this file's package root
#
if hasattr(sys, "_MEIPASS"):
    _BASE = sys._MEIPASS
    # Also expose the directory that contains the EXE itself
    _EXE_DIR = os.path.dirname(sys.executable)
else:
    _BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    _EXE_DIR = _BASE

os.environ.setdefault("DOCPRO_BASE_PATH", _BASE)
os.environ.setdefault("DOCPRO_EXE_DIR",  _EXE_DIR)

# ── 2.  CA certificate bundle for requests / urllib3 ─────────────────────────
_cacert = os.path.join(_BASE, "certifi", "cacert.pem")
if os.path.isfile(_cacert):
    os.environ["SSL_CERT_FILE"]    = _cacert
    os.environ["REQUESTS_CA_BUNDLE"] = _cacert

# ── 3.  Safe stdout/stderr when frozen without a console window ───────────────
#
#   PyInstaller sets sys.stdout / sys.stderr to None when --noconsole is used.
#   The logging module will crash on the first emit if we don't guard this.
#
class _NullWriter:
    """Absorbs all writes silently."""
    def write(self, *_):
        pass
    def flush(self):
        pass

if sys.stdout is None:
    sys.stdout = _NullWriter()
if sys.stderr is None:
    sys.stderr = _NullWriter()

# ── 4.  Working directory  ────────────────────────────────────────────────────
#
#   Windows launches EXEs with CWD = wherever the user double-clicked from.
#   We normalise to the EXE's own directory so relative paths in config.json
#   work predictably for end users.
#
try:
    os.chdir(_EXE_DIR)
except OSError:
    pass  # read-only filesystem edge case; ignore
