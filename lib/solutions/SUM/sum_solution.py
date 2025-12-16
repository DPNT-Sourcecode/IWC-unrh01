class SumSolution:
    def compute(self, x, y) -> int:
        if not isinstance(x, int) or not isinstance(y, int):
            raise TypeError("Parameter type is incorrect")
        return x + y



