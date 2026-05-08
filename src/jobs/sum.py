"""Simple calculation helpers."""


def calculate_sum(values):
	"""Return the sum of an iterable of numbers."""
	total = 0
	for value in values:
		total += value
	return total

