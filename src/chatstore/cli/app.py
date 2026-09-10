"""CLI surface. See PLAN.md "CLI" for the proposed command set and exit codes."""

import argparse
import sys

COMMANDS = [
    "doctor", "init", "sync", "status",
    "search", "chats", "people", "read", "resolve", "context",
    "archive", "rebuild-index",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chatstore")
    parser.add_argument("--data-dir", help="Override the user data directory.")
    parser.add_argument("--json", action="store_true", help="Emit the versioned JSON envelope.")
    sub = parser.add_subparsers(dest="command")
    for name in COMMANDS:
        sub.add_parser(name)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command is None:
        build_parser().print_help()
        return 2
    print(f"chatstore {args.command}: not implemented", file=sys.stderr)
    return 70  # EX_SOFTWARE placeholder until exit codes are specified
