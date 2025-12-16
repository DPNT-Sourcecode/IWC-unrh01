import pytest
from solutions.SUM.sum_solution import SumSolution


class TestSum:
    def test_sum(self):
        assert SumSolution().compute(1, 2) == 3

    def test_sum_negative(self):
        assert SumSolution().compute(-1, -2) == -3



