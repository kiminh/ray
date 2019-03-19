from __future__ import absolute_import
from __future__ import print_function

import math
import numpy as np
import unittest

import ray
import cython_examples as cyth


def get_ray_result(cython_func, *args):
    func = ray.remote(cython_func)
    return ray.get(func.remote(*args))


class CythonTest(unittest.TestCase):
    def setUp(self):
        cmd = "lsof /"
        print(cmd)
        os.system(cmd)
        cmd = "free -m"
        print(cmd)
        os.system(cmd)
        cmd = "df -h"
        print(cmd)
        os.system(cmd)
        cmd = "du -h -d /tmp"
        print(cmd)
        os.system(cmd)
        cmd = "ls -al /tmp/*"
        print(cmd)
        os.system(cmd)
        ray.init()

    def tearDown(self):
        ray.shutdown()

    def assertEqualHelper(self, cython_func, expected, *args):
        assert get_ray_result(cython_func, *args) == expected

    def test_simple_func(self):
        self.assertEqualHelper(cyth.simple_func, 6, 1, 2, 3)
        self.assertEqualHelper(cyth.fib, 55, 10)
        self.assertEqualHelper(cyth.fib_int, 55, 10)
        self.assertEqualHelper(cyth.fib_cpdef, 55, 10)
        self.assertEqualHelper(cyth.fib_cdef, 55, 10)

    def test_simple_class(self):
        cls = ray.remote(cyth.simple_class)
        a1 = cls.remote()
        a2 = cls.remote()

        result1 = ray.get(a1.increment.remote())
        result2 = ray.get(a2.increment.remote())
        result3 = ray.get(a2.increment.remote())

        assert result1 == 1
        assert result2 == 1
        assert result3 == 2

    def test_numpy(self):
        array = np.array([-1.0, 0.0, 1.0, 2.0])

        answer = [float("-inf") if x <= 0 else math.log(x) for x in array]
        result = get_ray_result(cyth.masked_log, array)

        np.testing.assert_array_equal(answer, result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
