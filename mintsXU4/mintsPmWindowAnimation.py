"""Compatibility entry point for :mod:`mintsWindowPdfAnimation`.

The animation now supports both PM and PC fields. Existing imports and commands
using this historical filename continue to work unchanged.
"""

try:  # package import (tests and library callers)
    from .mintsWindowPdfAnimation import *  # noqa: F401,F403
except ImportError:  # direct script execution from mintsXU4/
    from mintsWindowPdfAnimation import *  # noqa: F401,F403


if __name__ == "__main__":
    main()
