import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import errors  # noqa: E402

CONTRACT = {
    -1: errors.NodusError,
    -3: errors.NodusMemoryError,
    -4: errors.NodusStaleReadError,
    -5: errors.NodusCheckDepthError,
    -6: errors.NodusSchemaError,
    -7: errors.NodusLogBacklogError,
    -8: errors.NodusWriterFencedError,
    -9: errors.NodusCorruptLogError,
    -10: errors.NodusUnsupportedError,
    -11: errors.NodusUpgradeRequiredError,
    -12: errors.NodusIndeterminateError,
    -13: errors.NodusTokenLostError,
    -14: errors.NodusShipTimeoutError,
}


class ErrorContractTest(unittest.TestCase):
    def test_every_code_maps_to_its_own_exception(self):
        for code, expected in CONTRACT.items():
            with self.subTest(code=code):
                error = errors.error_for(code, "message")
                self.assertIs(expected, type(error))
                self.assertEqual(code, expected.code)
                self.assertEqual("message", str(error))

    def test_every_exception_is_a_nodus_error(self):
        for expected in CONTRACT.values():
            self.assertTrue(issubclass(expected, errors.NodusError))

    def test_the_memory_error_is_also_a_memory_error(self):
        self.assertTrue(issubclass(errors.NodusMemoryError, MemoryError))

    def test_a_ship_timeout_has_no_token_until_the_caller_attaches_one(self):
        error = errors.error_for(-14, "not shipped")

        self.assertIsNone(error.token)
        error.token = (3, 40)
        self.assertEqual((3, 40), error.token)

    def test_the_contract_is_contiguous_from_minus_one_to_minus_fourteen(self):
        self.assertEqual(set(range(-14, 0)) - {-2}, set(CONTRACT))

    def test_an_unknown_code_is_a_generic_error(self):
        self.assertIs(errors.NodusError, type(errors.error_for(-99, "unknown")))
        self.assertIs(errors.NodusError, type(errors.error_for(-2, "lookup")))


if __name__ == "__main__":
    unittest.main()
