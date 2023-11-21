import unittest

try:
    import helper  # type: ignore
except:
    import tests.helper  # type: ignore
import main  # type: ignore


class Test(unittest.TestCase):
    def test_tests(self):
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
