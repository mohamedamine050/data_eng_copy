import unittest

from src.jobs.sum import calculate_sum


class CalculateSumTests(unittest.TestCase):
    def test_calculate_sum_with_multiple_values(self):
        self.assertEqual(calculate_sum([1, 2, 3, 4]), 10)

    def test_calculate_sum_with_empty_input(self):
        self.assertEqual(calculate_sum([]), 0)


if __name__ == "__main__":
    unittest.main()