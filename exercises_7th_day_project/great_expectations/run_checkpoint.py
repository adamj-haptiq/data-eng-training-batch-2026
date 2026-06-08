"""CLI entrypoint for Great Expectations validation."""

import sys

from etl.gx_validation import run_validation


def main() -> int:
    success = run_validation()
    if success:
        print("GX validation success: True")
    else:
        print("ERROR: GX validation failed or raw_dividend_events table is empty")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
