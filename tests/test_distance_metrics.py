"""Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.
"""
import math

import pytest

from exercises.unit_test_demo.distance_metrics import great_circle_distance


def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    p1, p2 = (0, 0), (0, 0)
    northpole = (90, -22)  # on the poles, longitude has no meaning
    equator = (0, 89)

    assert great_circle_distance(p1[0], p1[1], p2[0], p2[1]) == 0
    expected = 6371 * math.pi / 2
    assert great_circle_distance(
        northpole[0], northpole[1], equator[0], equator[1]
    ) == pytest.approx(expected)
