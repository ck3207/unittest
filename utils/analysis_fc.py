# -*- coding: utf-8 -*-
import unittest
import logging
import time
import decimal

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations
from interfaces import interfaces
__author__ = "chenk"

class GetTradeStyle(unittest.TestCase):
    TABLE_NAME = "interval_trade_style"
    INTERFACE_NAME = "general/get_trade_style"
    COLUMNS = ["avg_hold_day","avg_hold_day_rank","drawdown","drawdown_rank","success_rate",
               "success_rate_rank","avg_position","avg_position_rank"]
    @classmethod
    def setUpClass(cls):
        database_info = get_configurations.get_target_section(section='database')
        cls.fc_info = get_configurations.get_target_section(section='fc_info')
        print("here is fc_info:\n", cls.fc_info)
        cls.conn = get_configurations.connect_to_mysql(database_info)
        cls.cur = cls.conn.cursor()

        cls.url_prefix = interfaces.get_url_prefix()
        cls.login_info = interfaces.request(url=cls.url_prefix+"user/login", data='{"user_token":"888888"}',
                                            is_get=False)

        print(cls.__class__.__name__, time.time())
        cls.data = {}
        # if fund_account_type == "0":
        #     self.data.setdefault("fundaccount", self.login_info.get("fundaccount")[0])
        # elif fund_account_type == "7":
        #     self.data.setdefault("fundaccountCredit", self.login_info.get("fundaccountCredit")[0])
        # elif fund_account_type.upper() == "B":
        #     self.data.setdefault("fundaccountOption", self.login_info.get("fundaccountOption")[0])
        cls.data.setdefault("user_token", cls.login_info.get("userToken"))
        cls.data.setdefault("fund_account", cls.login_info.get("fundaccount")[0])
        cls.data.setdefault("fund_account_type", "0")




    def test_normal(self):
        """
        :param
        {
  "fund_account": "1,11****19",
  "fund_account_type": "0",
  "interval": "1",
  "user_token": "888888"
}
        
        :return: 
                {
  "error_no": "0",
  "error_info": "执行成功",
  "avg_hold_day": 1012,
  "avg_hold_day_rank": 0.188,
  "drawdown": 0.0277,
  "drawdown_rank": null,
  "success_rate": 0.2788,
  "success_rate_rank": 0.1396,
  "avg_position": 0.7152,
  "avg_position_rank": 0.0849
}
        """
        url = self.url_prefix + GetTradeStyle.INTERFACE_NAME
        self.data.setdefault("interval", "1")
        data = str(self.data).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get=False)
        sql_tmp = "select "
        for column in GetTradeStyle.COLUMNS:
            sql_tmp += column + ", "
        sql = sql_tmp[:-2] + " from {0} where fund_account = {1} and interval_type = {2};"\
            .format(GetTradeStyle.TABLE_NAME, self.fc_info["fund_account"], self.fc_info["interval_type"])
        print("Will execute sql: \n", sql)
        self.cur.execute(sql)
        sql_result = self.cur.fetchone()
        print("sql_result:\n", sql_result)
        for i, column in enumerate(GetTradeStyle.COLUMNS):
            if isinstance(sql_result[i], decimal.Decimal):
                self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)))
                # print(sql_result[i], interface_result.get(column))
            else:
                self.assertEqual(sql_result[i], interface_result.get(column))

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()

if __name__ == "__main__":
    tests = unittest.TestSuite(unittest.makeSuite(testCaseClass=GetTradeStyle, prefix='test'))
    # tests = unittest.TestSuite()
    # tests.addTest(GetTradeStyle('test_normal'))
    with open(r"result.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

