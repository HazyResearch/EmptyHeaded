import unittest
from codegenerator.fft.genFFT import GenFFT
import os


class GenFFTTest(unittest.TestCase):
    def test_basic(self):
        gen = GenFFT(2, 3)
        self.assertEqual(8, gen.n)
        fname = gen.gen()
        with open(fname, 'r') as f:
            lines = f.readlines()
            self.assertGreater(len(lines), 0)
