#!/usr/bin/env python3
"""
build_scripts/build.py
----------------------
Production build pipeline for DocPro Desktop Agent.

Usage
-----
    # Standard PyInstaller build (recommended for most users)
    python build_scripts/build.py

    # Nuitka build (smaller binary, fewer AV false-positives)
    python build_scripts/build.py --backend nuitka

    # Debug build (console window + extra logging, no UPX compression)
    python build_scripts/build.py --debug

    # Skip packaging into a zip archive
    python build_scripts/build.py --no-zip

    # Code-sign the EXE after build (requires signtool.exe on PATH)
    python build_scripts/build.py --sign --cert "My Code Signing Cert"

Exit codes
----------
    0  success
    1  environment problem (missing tool / wrong Python version)
    2  build failure
    3  post-build packaging failure
"""

import argparse
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

# -- PROJECT LAYOUT ------------------------------------------------------------

ROOT        = Path(__file__).resolve().parent.parent   # repo root
BUILD_DIR   = ROOT / "build_scripts"
DIST_DIR    = ROOT / "dist"
AGENT_ENTRY = ROOT / "docpro_agent" / "main.py"
SPEC_FILE   = ROOT / "docpro_agent.spec"
ICON_FILE   = ROOT / "assets" / "icon.ico"
EXAMPLE_CFG = ROOT / "config.example.json"
README_FILE = ROOT / "README.txt"

AGENT_NAME  = "docpro_agent"
VERSION     = "1.1.0"

# -- HELPERS -------------------------------------------------------------------

def banner(msg: str) -> None:
    width = 72
    print("\n" + "-" * width)
    print(f"  {msg}")
    print("-" * width)


def run(cmd: list[str], *, cwd: Path = ROOT, check: bool = True) -> subprocess.CompletedProcess:
    """Run a subprocess, stream output live, and raise on failure."""
    print(f"\n$ {' '.join(str(c) for c in cmd)}\n")
    result = subprocess.run(cmd, cwd=str(cwd), check=False)
    if check and result.returncode != 0:
        print(f"\n[ERROR] Command failed with exit code {result.returncode}")
        sys.exit(2)
    return result


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def require_python() -> None:
    banner("Checking Python version")
    major, minor = sys.version_info[:2]
    print(f"  Python {major}.{minor} detected")
    if (major, minor) < (3, 9):
        print("[ERROR] Python 3.9+ is required.")
        sys.exit(1)
    print("  [OK] OK")


def require_tool(name: str, install_hint: str) -> str:
    """Return the resolved path to *name* or abort with an install hint."""
    path = shutil.which(name)
    if not path:
        print(f"\n[ERROR] '{name}' not found on PATH.\n  Install with:  {install_hint}")
        sys.exit(1)
    print(f"  [OK] {name} -> {path}")
    return path


def require_package(package: str) -> None:
    try:
        __import__(package)
        print(f"  [OK] {package}")
    except ImportError:
        print(f"\n[ERROR] Python package '{package}' is not installed.")
        print(f"  Run:  pip install {package}")
        sys.exit(1)


# -- ENVIRONMENT CHECK ---------------------------------------------------------

def check_environment(backend: str) -> None:
    banner("Checking build environment")
    require_python()

    print("\n  Python packages:")
    require_package("requests")
    require_package("urllib3")

    print("\n  Build tools:")
    if backend == "pyinstaller":
        require_tool("pyinstaller", "pip install pyinstaller")
    elif backend == "nuitka":
        require_tool("python", "already installed")
        require_package("nuitka")

    if not AGENT_ENTRY.exists():
        print(f"\n[ERROR] Entry point not found: {AGENT_ENTRY}")
        sys.exit(1)
    print(f"  [OK] Entry point: {AGENT_ENTRY.relative_to(ROOT)}")


# -- CLEAN ---------------------------------------------------------------------

def clean_previous_build() -> None:
    banner("Cleaning previous build artefacts")
    for target in (DIST_DIR / AGENT_NAME, DIST_DIR / f"{AGENT_NAME}_portable.exe"):
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
            print(f"  Removed: {target.relative_to(ROOT)}")

    # Remove stale PyInstaller work dirs
    work = ROOT / "build"
    if work.exists():
        shutil.rmtree(work)
        print(f"  Removed: build/")

    print("  [OK] Clean")


# -- PYINSTALLER BUILD ---------------------------------------------------------

def build_pyinstaller(debug: bool) -> None:
    banner("Building with PyInstaller")

    if SPEC_FILE.exists():
        # Preferred: use the pre-configured spec file
        cmd = ["pyinstaller", "--clean", str(SPEC_FILE)]
    else:
        # Fallback: inline flags when spec is missing
        cmd = [
            "pyinstaller",
            "--clean",
            "--onefile",
            "--name", AGENT_NAME,
            "--hidden-import=requests",
            "--hidden-import=requests.adapters",
            "--hidden-import=urllib3",
            "--hidden-import=urllib3.util.retry",
            "--hidden-import=certifi",
            "--hidden-import=charset_normalizer",
            "--hidden-import=idna",
            "--hidden-import=uuid",
            "--hidden-import=zipfile",
            "--hidden-import=hashlib",
            "--hidden-import=signal",
            "--hidden-import=argparse",
            "--hidden-import=dataclasses",
            "--hidden-import=logging.handlers",
        ]

        if EXAMPLE_CFG.exists():
            sep = ";" if platform.system() == "Windows" else ":"
            cmd += ["--add-data", f"{EXAMPLE_CFG}{sep}."]

        if ICON_FILE.exists():
            cmd += ["--icon", str(ICON_FILE)]

        if debug:
            cmd += ["--console", "--log-level", "DEBUG"]
        else:
            cmd += ["--console"]         # keep console for log visibility

        if not debug:
            cmd += ["--upx-dir", "upx"] if shutil.which("upx") else []

        rt_hook = BUILD_DIR / "rthook_set_paths.py"
        if rt_hook.exists():
            cmd += ["--runtime-hook", str(rt_hook)]

    run(cmd)
    print("\n  [OK] PyInstaller build complete")


# -- NUITKA BUILD --------------------------------------------------------------

def build_nuitka(debug: bool) -> None:
    banner("Building with Nuitka")

    # Nuitka outputs to the CWD by default; redirect to dist/
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "nuitka",
        "--standalone",
        "--onefile",
        f"--output-dir={DIST_DIR}",
        f"--output-filename={AGENT_NAME}.exe",
        "--assume-yes-for-downloads",       # auto-download Nuitka's C compiler helper
        "--follow-imports",
        "--include-package=requests",
        "--include-package=urllib3",
        "--include-package=certifi",
        "--include-package=charset_normalizer",
        "--include-package=idna",
        "--enable-plugin=anti-bloat",       # strips unused stdlib modules
        "--nofollow-import-to=tkinter",
        "--nofollow-import-to=matplotlib",
        "--nofollow-import-to=numpy",
        "--nofollow-import-to=PIL",
    ]

    if ICON_FILE.exists():
        cmd += [f"--windows-icon-from-ico={ICON_FILE}"]

    if not debug:
        cmd += ["--windows-console-mode=disable"]
        cmd += ["--lto=yes"]               # link-time optimisation
    else:
        cmd += ["--windows-console-mode=force"]

    if EXAMPLE_CFG.exists():
        cmd += [f"--include-data-files={EXAMPLE_CFG}=config.example.json"]

    cmd.append(str(AGENT_ENTRY))
    run(cmd)
    print("\n  [OK] Nuitka build complete")


# -- CODE SIGNING --------------------------------------------------------------

def sign_exe(exe_path: Path, cert_name: str) -> None:
    """Sign *exe_path* using signtool.exe (Windows SDK required)."""
    banner(f"Code-signing {exe_path.name}")

    signtool = shutil.which("signtool")
    if not signtool:
        # Try common SDK locations
        sdk_roots = [
            Path(r"C:\Program Files (x86)\Windows Kits\10\bin"),
            Path(r"C:\Program Files\Windows Kits\10\bin"),
        ]
        for root in sdk_roots:
            if root.exists():
                matches = sorted(root.glob("*/x64/signtool.exe"), reverse=True)
                if matches:
                    signtool = str(matches[0])
                    break

    if not signtool:
        print("[WARNING] signtool.exe not found - skipping code signing.")
        return

    cmd = [
        signtool, "sign",
        "/n", cert_name,
        "/t", "http://timestamp.digicert.com",
        "/fd", "SHA256",
        "/v",
        str(exe_path),
    ]
    result = run(cmd, check=False)
    if result.returncode == 0:
        print("  [OK] Signed successfully")
    else:
        print("[WARNING] Signing failed - distributing unsigned EXE.")


# -- POST-BUILD VERIFICATION ---------------------------------------------------

def verify_outputs() -> list[Path]:
    """Return a list of all EXE files produced, or abort if none found."""
    banner("Verifying build outputs")

    exes: list[Path] = []

    # One-file portable output (Unified Super Agent)
    portable_exe = DIST_DIR / f"{AGENT_NAME}.exe"
    if portable_exe.exists():
        exes.append(portable_exe)
        size_mb = portable_exe.stat().st_size / 1024 / 1024
        print(f"  [OK] {portable_exe.relative_to(ROOT)}  ({size_mb:.1f} MB)")

    if not exes:
        print("\n[ERROR] No EXE files found in dist/. Build may have failed silently.")
        sys.exit(2)

    return exes


# -- ZIP RELEASE PACKAGE -------------------------------------------------------

def package_release(exes: list[Path], sign: bool, cert: str) -> Path:
    """
    Assemble a distributable ZIP:
        docpro_release_v{VERSION}_{date}.zip
        ├-- docpro_agent.exe
        ├-- docpro_agent_portable.exe  (if built)
        ├-- config.example.json
        ├-- README.txt
        └-- checksums.sha256
    """
    banner("Packaging release ZIP")

    date_str   = datetime.now().strftime("%Y%m%d")
    zip_name   = f"docpro_release_v{VERSION}_{date_str}.zip"
    zip_path   = DIST_DIR / zip_name
    release_dir = DIST_DIR / f"docpro_release_v{VERSION}"

    release_dir.mkdir(parents=True, exist_ok=True)

    checksums: list[str] = []

    for exe in exes:
        dest = release_dir / exe.name
        shutil.copy2(exe, dest)
        if sign and cert:
            sign_exe(dest, cert)
        chk = sha256_file(dest)
        checksums.append(f"{chk}  {exe.name}")
        print(f"  + {exe.name}  SHA256={chk[:16]}...")

    if EXAMPLE_CFG.exists():
        shutil.copy2(EXAMPLE_CFG, release_dir / "config.example.json")
        print(f"  + config.example.json")

    # Generate README.txt if not present
    readme_dest = release_dir / "README.txt"
    if README_FILE.exists():
        shutil.copy2(README_FILE, readme_dest)
    else:
        readme_dest.write_text(
            f"DocPro Desktop Agent v{VERSION}\n"
            "===============================\n\n"
            "Quick Start\n"
            "-----------\n"
            "1. Copy config.example.json -> config.json\n"
            "2. Fill in api_base, device_id, api_token, download_dir\n"
            "3. Run docpro_agent.exe\n\n"
            "Files\n"
            "-----\n"
            "  docpro_agent.exe          Standard build (fastest start)\n"
            "  docpro_agent_portable.exe Single-file portable build\n"
            "  config.example.json       Configuration template\n\n"
            "Command-line Options\n"
            "--------------------\n"
            "  --config <path>    Path to config.json\n"
            "  --bundle <id>      Claim & process a specific bundle, then poll\n\n"
            "Support\n"
            "-------\n"
            "  https://your-support-url-here\n",
            encoding="utf-8",
        )
    print(f"  + README.txt")

    # Write checksum manifest
    chk_file = release_dir / "checksums.sha256"
    chk_file.write_text("\n".join(checksums) + "\n", encoding="utf-8")
    print(f"  + checksums.sha256")

    # Zip it up
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for item in release_dir.rglob("*"):
            if item.is_file():
                zf.write(item, arcname=item.relative_to(release_dir))

    zip_size_mb = zip_path.stat().st_size / 1024 / 1024
    print(f"\n  [OK] Release archive: {zip_path.relative_to(ROOT)}  ({zip_size_mb:.1f} MB)")
    return zip_path


# -- ENTRY POINT ---------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build DocPro Desktop Agent into a standalone Windows EXE.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--backend",
        choices=["pyinstaller", "nuitka"],
        default="pyinstaller",
        help="Build backend (default: pyinstaller)",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Build with console window + debug output (no UPX compression)",
    )
    p.add_argument(
        "--no-zip",
        action="store_true",
        dest="no_zip",
        help="Skip creating the release ZIP archive",
    )
    p.add_argument(
        "--sign",
        action="store_true",
        help="Code-sign the EXE with signtool.exe (Windows SDK required)",
    )
    p.add_argument(
        "--cert",
        default="",
        metavar="NAME",
        help="Certificate subject name for --sign (e.g. 'My Company Ltd')",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"\n{'='*72}")
    print(f"  DocPro Desktop Agent - Build Pipeline   v{VERSION}")
    print(f"  Backend: {args.backend.upper()}   Debug: {args.debug}   Sign: {args.sign}")
    print(f"{'='*72}")

    check_environment(args.backend)
    clean_previous_build()

    if args.backend == "pyinstaller":
        build_pyinstaller(debug=args.debug)
    elif args.backend == "nuitka":
        build_nuitka(debug=args.debug)

    exes = verify_outputs()

    if not args.no_zip:
        try:
            zip_path = package_release(exes, sign=args.sign, cert=args.cert)
        except Exception as exc:
            print(f"\n[ERROR] Packaging failed: {exc}")
            sys.exit(3)
    elif args.sign and args.cert:
        for exe in exes:
            sign_exe(exe, args.cert)

    banner("Build complete [OK]")
    print(f"\n  Output directory: {DIST_DIR.relative_to(ROOT)}/\n")
    for exe in exes:
        print(f"    {exe.relative_to(ROOT)}")
    if not args.no_zip:
        print(f"\n  Release archive ready for distribution.")
    print()


if __name__ == "__main__":
    main()
