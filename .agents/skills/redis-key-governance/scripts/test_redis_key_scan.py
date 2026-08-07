import tempfile
import unittest
from pathlib import Path

from redis_key_scan import (
    is_allowed,
    looks_like_key_literal,
    scan_file,
    should_scan_line,
    string_literals,
    walk_files,
)


class RedisKeyScanTest(unittest.TestCase):
    def test_relative_helper_path_is_allowed(self):
        self.assertTrue(is_allowed("common/rediskeys/user.go"))

    def test_lua_comment_is_ignored(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, "script.lua")
            path.write_text('-- redis key "danger:*"\nreturn 1\n', encoding="utf-8")
            self.assertEqual(scan_file(str(path)), [])

    def test_inline_key_outside_helper_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, "logic.go")
            path.write_text('package logic\nconst cacheKey = "user:profile"\n', encoding="utf-8")
            findings = scan_file(str(path))
            self.assertEqual(len(findings), 1)
            self.assertIn("possible inline Redis key literal", findings[0][2])

    def test_route_and_log_strings_are_not_key_context(self):
        self.assertFalse(should_scan_line('t.Fatalf("route missing: %s", key)'))
        self.assertFalse(should_scan_line('Path: "/api/items/:id",'))
        self.assertFalse(looks_like_key_literal("***"))

    def test_redis_operation_literal_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, "logic.go")
            path.write_text(
                'package logic\nfunc save() { rds.Set(ctx, "user:session", value, ttl) }\n',
                encoding="utf-8",
            )
            findings = scan_file(str(path))
            self.assertEqual(len(findings), 1)

    def test_extended_redis_operations_are_scanned(self):
        for operation in ("Incr", "Exists", "HGetAll", "XAdd", "Publish", "TTL"):
            with self.subTest(operation=operation):
                line = f'rds.{operation}(ctx, "rate:limit")'
                self.assertTrue(should_scan_line(line))

    def test_lua_keys_call_is_reported(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root, "script.lua")
            path.write_text("return redis.call('KEYS', 'user:*')\n", encoding="utf-8")
            findings = scan_file(str(path))
            messages = [finding[2] for finding in findings]
            self.assertTrue(any("Lua Redis scan/keys" in message for message in messages))
            self.assertTrue(any("inline Redis key" in message for message in messages))

    def test_lua_single_quoted_key_is_extracted(self):
        self.assertIn("user:profile", string_literals("redis.call('GET', 'user:profile')"))

    def test_walk_skips_go_tests_by_default(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "logic.go").write_text("package logic\n", encoding="utf-8")
            Path(root, "logic_test.go").write_text("package logic\n", encoding="utf-8")
            self.assertEqual(len(list(walk_files(root))), 1)
            self.assertEqual(len(list(walk_files(root, include_tests=True))), 2)


if __name__ == "__main__":
    unittest.main()
