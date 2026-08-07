#!/usr/bin/env python3
"""Check that project-owned YAML mapping fields have adjacent Chinese comments."""

import argparse
import os
import re
import sys
from pathlib import Path

HAN_RE = re.compile(r"[\u3400-\u9fff]")
SKIP_DIRS = {".git", "bin", "build", "coverage", "dist", "node_modules", "vendor"}


def mapping_field(line: str):
    """Return key, indentation and value for a block-style YAML mapping field."""
    indent = len(line) - len(line.lstrip(" "))
    content = line[indent:]
    if not content or content.startswith(("#", "---", "...", "%")):
        return None
    if content.startswith("- "):
        content = content[2:].lstrip(" ")
    elif content == "-":
        return None

    quote = None
    for index, char in enumerate(content):
        if quote:
            if char == quote and (index == 0 or content[index - 1] != "\\"):
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
            continue
        if char != ":":
            continue
        if index + 1 < len(content) and not content[index + 1].isspace():
            continue
        key = content[:index].strip()
        if not key or key.startswith(("{", "[", "?")):
            return None
        if key[:1] == key[-1:] and key[:1] in ("'", '"'):
            key = key[1:-1]
        return key, indent, content[index + 1 :].strip()
    return None


def has_adjacent_chinese_comment(lines, index: int, indent: int) -> bool:
    """Require the immediately preceding line to be a same-indent Chinese comment."""
    if index == 0:
        return False
    previous = lines[index - 1]
    previous_indent = len(previous) - len(previous.lstrip(" "))
    return (
        previous_indent == indent
        and previous.lstrip(" ").startswith("#")
        and HAN_RE.search(previous) is not None
    )


def scan_lines(lines, dynamic_parents=()):
    """Return line, field path and message for missing YAML field comments."""
    dynamic = set(dynamic_parents)
    stack = []
    findings = []
    block_indent = None

    for index, line in enumerate(lines):
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if block_indent is not None:
            if not stripped or indent > block_indent:
                continue
            block_indent = None
        if not stripped or stripped.startswith("#"):
            continue

        field = mapping_field(line)
        if field is None:
            continue
        key, indent, value = field
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent_path = ".".join(item[1] for item in stack)
        field_path = ".".join([*(item[1] for item in stack), key])
        if parent_path not in dynamic and not has_adjacent_chinese_comment(lines, index, indent):
            findings.append((index + 1, field_path, "missing adjacent same-indent Chinese comment"))
        if value == "":
            stack.append((indent, key))
        elif value.startswith(("|", ">")):
            block_indent = indent
    return findings


def yaml_files(paths):
    """Yield explicit YAML files and YAML files below explicit directories."""
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_file():
            if path.suffix.lower() in (".yaml", ".yml"):
                yield path
            continue
        if not path.is_dir():
            raise FileNotFoundError(raw_path)
        for root, dirs, files in os.walk(path):
            dirs[:] = sorted(name for name in dirs if name not in SKIP_DIRS)
            for name in sorted(files):
                candidate = Path(root, name)
                if candidate.suffix.lower() in (".yaml", ".yml"):
                    yield candidate


def main(argv=None) -> int:
    """Run the YAML comment audit and return a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", help="project-owned YAML file or directory")
    parser.add_argument(
        "--dynamic-parent",
        action="append",
        default=[],
        help="dot path whose direct mapping keys are dynamic data; may be repeated",
    )
    parser.add_argument("--advisory-exit-zero", action="store_true")
    args = parser.parse_args(argv)

    count = 0
    try:
        files = list(yaml_files(args.paths))
    except FileNotFoundError as error:
        print(f"path not found: {error}", file=sys.stderr)
        return 2
    for path in files:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line, field_path, message in scan_lines(lines, args.dynamic_parent):
            count += 1
            print(f"{path}:{line}: {message}: {field_path}")
    if count and not args.advisory_exit_zero:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
