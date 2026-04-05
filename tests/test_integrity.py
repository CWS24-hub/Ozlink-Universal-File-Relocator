import tempfile
import unittest
from pathlib import Path

from ozlink_console.integrity import sha256_file, verify_copied_file, verify_copied_tree


class IntegrityTests(unittest.TestCase):
    def test_sha256_file(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "t.bin"
            p.write_bytes(b"abc")
            self.assertEqual(
                sha256_file(p),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )

    def test_verify_copied_file_ok(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            s = td / "s.txt"
            d = td / "d.txt"
            s.write_text("x", encoding="utf-8")
            d.write_text("x", encoding="utf-8")
            ok, msg, hs, hd = verify_copied_file(s, d)
            self.assertTrue(ok)
            self.assertEqual(hs, hd)

    def test_verify_copied_tree(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src = td / "src"
            dst = td / "dst"
            (src / "sub").mkdir(parents=True)
            (src / "sub" / "a.txt").write_text("1", encoding="utf-8")
            (dst / "sub").mkdir(parents=True)
            (dst / "sub" / "a.txt").write_text("1", encoding="utf-8")
            ok, msg = verify_copied_tree(src, dst)
            self.assertTrue(ok, msg)
