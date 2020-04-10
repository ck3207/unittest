import unittest
import os

from HTMLTestRunner import HTMLTestRunner


class Demo(unittest.TestCase):
    """测试 Demo 类"""

    def test_a(self):
        self.assertEqual('1', '1')

    def test_b(self):
        self.assertEqual('1', '2')

    def test_c(self):
        self.assertEqual('1', '1')


def suite():
    pass


if __name__ == "__main__":
    print(os.getcwd())
    tests = unittest.TestSuite(unittest.makeSuite(Demo, 'test'))
    # test = unittest.TestSuite()
    # test.addTest(Demo('test_a'))
    # # unittest.TextTestRunner.run(tests)
    with open(r"E:\scripts\git\unittest\result.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：').run(tests)
