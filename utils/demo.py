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


def generate_nums(start=777700, nums=100):
    fund_account = start
    with open(file="fund_accounts", mode="w") as f:
        while nums:
            f.write("{}\n".format(fund_account))
            fund_account += 1
            nums -= 1
    print("Generate Finished!")
    return


if __name__ == "__main__":
    print(os.getcwd())
    # tests = unittest.TestSuite(unittest.makeSuite(Demo, 'test'))
    # test = unittest.TestSuite()
    # test.addTest(Demo('test_a'))
    # # unittest.TextTestRunner.run(tests)
    # with open(r"demo.html", "wb") as f:
    #     HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：').run(tests)
    generate_nums(start=777700, nums=30)
