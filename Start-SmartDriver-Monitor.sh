#!/bin/sh
set -eu

cd "$(dirname "$0")"

command -v python3 >/dev/null 2>&1 || {
    echo "Python 3 is required." >&2
    exit 127
}

python3 -c 'import tkinter' >/dev/null 2>&1 || {
    echo "Tkinter is unavailable." >&2
    echo "Ubuntu/Debian: sudo apt install python3-tk fonts-noto-cjk" >&2
    exit 2
}

exec python3 smartdriver_monitor.py "$@"
