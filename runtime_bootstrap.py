"""Initialize the platform session home, then start the runtime as appuser.

Run with Python's isolated mode: no session-owned Python code may run before
the process has permanently dropped its startup privileges.
"""

import os
from pathlib import Path


def main() -> None:
    home = os.environ.get("HOME", "/home/appuser")
    if home not in {"/home/session", "/home/appuser"}:
        raise RuntimeError("Unsupported managed runtime home")
    descriptor = os.open(home, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.geteuid() == 0:
            # Change only the mounted home itself; never follow or recursively
            # change session files, which the runtime manages as appuser.
            os.fchown(descriptor, 10001, 10001)
    finally:
        os.close(descriptor)
    if os.geteuid() == 0:
        os.setgroups([])
        os.setresgid(10001, 10001, 10001)
        os.setresuid(10001, 10001, 10001)
    if os.getuid() != 10001 or os.getgid() != 10001:
        raise RuntimeError("Managed runtime requires appuser")
    if not os.access(Path(home), os.W_OK | os.X_OK):
        raise RuntimeError("Managed runtime home is not writable")
    os.environ["PYTHONNOUSERSITE"] = "1"
    os.execv("/usr/local/bin/andromeda-runtime-server", ["andromeda-runtime-server"])


if __name__ == "__main__":
    main()
