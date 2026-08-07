#!/usr/bin/env python3
import argparse
import os
import re
import sys

SKIP_DIRS = {".git", "vendor", "node_modules", "dist", "build", "bin", ".turbo", "coverage"}
ALLOWED_PARTS = (
    ("common", "keys"),
    ("common", "rediskeys"),
    ("data", "keys"),
    ("internal", "rediskeys"),
    ("pkg", "rediskeys"),
)
STRING_RE = re.compile(r'(["`])([^"`]{2,})\1')
LUA_STRING_RE = re.compile(r"(['\"])([^'\"]{2,})\1")
KEY_ASSIGNMENT_RE = re.compile(
    r"\b(?:key\w*|[A-Za-z_]\w*(?:Key|Keys|KeyPrefix|KeyPattern|KeyTemplate|_key|_keys|_key_prefix|_key_pattern|_key_template))\s*(?::=|=)",
)
REDIS_CONTEXT_RE = re.compile(r"(?:\b(?:redis|rds|cache|pipe)\w*\b|\.Redis\s*\()", re.IGNORECASE)
REDIS_OPERATION_RE = re.compile(
    r"\.(?:"
    r"Set|SetNX|SetXX|Get|GetDel|GetSet|MGet|MSet|MSetNX|Del|Unlink|Exists|Expire|ExpireAt|PExpire|TTL|PTTL|Persist|Rename|"
    r"Incr|IncrBy|Decr|DecrBy|"
    r"HSet|HGet|HGetAll|HMGet|HDel|HExists|HIncrBy|HKeys|HVals|HLen|"
    r"SAdd|SRem|SMembers|SIsMember|SCard|SPop|"
    r"ZAdd|ZRem|ZRange|ZRangeByScore|ZScore|ZCard|ZIncrBy|"
    r"LPush|RPush|LPop|RPop|LRange|LTrim|LLen|"
    r"XAdd|XRead|XReadGroup|XAck|XDel|XTrim|Publish|Subscribe|PSubscribe|"
    r"Eval|EvalSha|ScriptLoad|Scan|ScanType|HScan|SScan|ZScan|Keys|Do|Pipeline|Pipelined|TxPipeline|TxPipelined"
    r")\s*\(",
)
REDIS_SCAN_RE = re.compile(r"\.(?:Scan|ScanType|HScan|SScan|ZScan|Keys)\s*\(")
LUA_REDIS_CALL_RE = re.compile(
    r"\bredis\.(?:call|pcall)\s*\(\s*(['\"])([A-Za-z]+)\1", re.IGNORECASE
)
LUA_SCAN_COMMANDS = {"KEYS", "SCAN", "HSCAN", "SSCAN", "ZSCAN"}


def is_allowed(path: str) -> bool:
    parts = tuple(part for part in os.path.normpath(path).split(os.sep) if part not in ("", "."))
    return any(
        parts[index : index + len(allowed)] == allowed
        for allowed in ALLOWED_PARTS
        for index in range(len(parts) - len(allowed) + 1)
    )


def should_scan_line(line: str) -> bool:
    code = STRING_RE.sub('""', line)
    if KEY_ASSIGNMENT_RE.search(code):
        return True
    if LUA_REDIS_CALL_RE.search(line):
        return True
    return bool(REDIS_CONTEXT_RE.search(code) and REDIS_OPERATION_RE.search(code))


def looks_like_key_literal(literal: str) -> bool:
    if ":" in literal:
        return True
    return "*" in literal and bool(re.search(r"[A-Za-z0-9_:-]", literal))


def string_literals(line: str):
    """Extract Go and Lua string literals without returning duplicates."""
    literals = [literal for _, literal in STRING_RE.findall(line)]
    for _, literal in LUA_STRING_RE.findall(line):
        if literal not in literals:
            literals.append(literal)
    return literals


def walk_files(root: str, include_tests: bool = False):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        for name in filenames:
            if name.endswith("_test.go") and not include_tests:
                continue
            if name.endswith((".go", ".lua")):
                yield os.path.join(dirpath, name)


def scan_file(path: str):
    findings = []
    allowed = is_allowed(path)
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            for lineno, line in enumerate(handle, 1):
                stripped = line.strip()
                if not stripped or stripped.startswith(("//", "--")):
                    continue
                code = STRING_RE.sub('""', line)
                if REDIS_CONTEXT_RE.search(code) and REDIS_SCAN_RE.search(code):
                    findings.append((path, lineno, "wildcard Redis scan/keys call needs explicit review"))
                lua_call = LUA_REDIS_CALL_RE.search(line)
                if lua_call and lua_call.group(2).upper() in LUA_SCAN_COMMANDS:
                    findings.append((path, lineno, "Lua Redis scan/keys call needs explicit review"))
                if allowed or not should_scan_line(line):
                    continue
                for literal in string_literals(line):
                    if looks_like_key_literal(literal):
                        findings.append((path, lineno, f"possible inline Redis key literal {literal!r}"))
    except OSError as exc:
        findings.append((path, 1, str(exc)))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Advisory Redis key governance scan.")
    parser.add_argument("root", nargs="?", default=".")
    parser.add_argument("--advisory-exit-zero", action="store_true")
    parser.add_argument("--include-tests", action="store_true")
    args = parser.parse_args()

    findings = []
    for path in walk_files(args.root, include_tests=args.include_tests):
        findings.extend(scan_file(path))

    for path, lineno, message in findings:
        print(f"{path}:{lineno}: {message}")

    if findings and not args.advisory_exit_zero:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
