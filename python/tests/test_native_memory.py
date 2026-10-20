import ctypes
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import NodusError, _native  # noqa: E402


def _library():
    try:
        library, isolate = _native.load()
    except NodusError:
        return None, None
    if not hasattr(library, "nodus_selftest_memory"):
        return None, None
    library.nodus_selftest_memory.restype = ctypes.c_int
    library.nodus_selftest_memory.argtypes = [_native.THREAD]
    return library, isolate


LIBRARY, ISOLATE = _library()


@unittest.skipIf(LIBRARY is None, "libnodusdb lacks nodus_selftest_memory; rebuild the native library")
class NativeMemorySelfTest(unittest.TestCase):
    def test_the_native_library_reads_memory_through_checked_pointers(self):
        thread = _native.current_thread(LIBRARY, ISOLATE)

        failures = LIBRARY.nodus_selftest_memory(thread)

        self.assertEqual(0, failures, _native.last_error_message(LIBRARY, thread))


if __name__ == "__main__":
    unittest.main()
