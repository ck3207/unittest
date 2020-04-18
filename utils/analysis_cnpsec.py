# -*- coding: utf-8 -*-
import unittest
import logging
import time, datetime
import decimal

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations
from business.interfaces import interfaces
__author__ = "chenk"

class GetMonthBillIncome(unittest.TestCase):
    TABLE_NAME = "month_interval_asset_change"
    INTERFACE_NAME = "month_bill/get_month_bill_income"
    COLUMNS = ["asset_income", "asset_yield", "stock_income", "wit_income", "fund_income", 
    "other_income", "draw_back", "begin_asset", "last_asset", "fund_in", "fund_out", "fund_rank"]
    @classmethod
    def setUpClass(cls):
        database_info = get_configurations.get_target_section(section='database_cnpsec')
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.conn = get_configurations.connect_to_mysql(database_info)
        cls.cur = cls.conn.cursor()

        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")
        # cls.login_info = interfaces.request(url=cls.url_prefix+"user/login", data='{"user_token":"888888"}',
                                            # is_get_method=False)

        print(cls.__class__.__name__, time.time())
        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetMonthBillIncome.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        sql_tmp = "select "
        for column in GetMonthBillIncome.COLUMNS:
            sql_tmp += column + ", "
        sql = sql_tmp[:-2] + " from {0} where fund_account = {1} and interval_type = {2};"\
            .format(GetMonthBillIncome.TABLE_NAME, self.info["fund_account"], "5")
        print("Will execute sql: \n", sql)
        self.cur.execute(sql)
        sql_result = self.cur.fetchone()
        print("sql_result:\n", sql_result)
        for i, column in enumerate(GetMonthBillIncome.COLUMNS):
            try:
                if isinstance(sql_result[i], decimal.Decimal):
                    self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)))
                else:
                    self.assertEqual(sql_result[i], interface_result.get(column))
            except TypeError:
                self.assertTrue(0, msg="sql is {0}\n interface is {1}\n params is {2}\n\
                Current is checking column {3}".format(sql, url, data, column))

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()


class ListMonthBankChange(unittest.TestCase):
    TABLE_NAME = "daily_asset"
    INTERFACE_NAME = "month_bill/list_month_bank_change"
    COLUMNS = ["init_date", "asset", "fund_in", "fund_out"]
    
    @classmethod
    def setUpClass(cls):
        database_info = get_configurations.get_target_section(section='database_cnpsec')
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.conn = get_configurations.connect_to_mysql(database_info)
        cls.cur = cls.conn.cursor()

        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")
        # cls.login_info = interfaces.request(url=cls.url_prefix+"user/login", data='{"user_token":"888888"}',
                                            # is_get_method=False)

        print(cls.__class__.__name__, time.time())
        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + ListMonthBankChange.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # SQL
        sql = "SELECT UNIX_TIMESTAMP(init_date) as init_date, asset, fund_in, fund_out from daily_asset \
        where fund_account = {0} and substr(init_date from 1 for 6) = {1} \
        order by init_date;".format(self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)
        self.cur.execute(sql)
        sql_result = self.cur.fetchall()
        print("sql_result:\n", sql_result)
        # SQL 执行结果为多条数据时，需取每一条数据，对比每一条记录里面的字段值
        for sql_index, sql_data in enumerate(sql_result):
            # 从 SQL 中获取的字段， 按照顺序比对每一个字段的值
            for column_index, column in enumerate(ListMonthBankChange.COLUMNS):
                try:
                    interface_data = interface_result.get("data_list")[sql_index].get(column)
                    msg="\nSQL is {0}\n Interface is {1}\n params is {2}\nCurrent is checking column {3}\n\
                    Interface response is {4}".format(sql, url, data, column, interface_result)
                    # 该字段为时间戳， 且接口时间戳字段比 SQL 查询的时间戳字段 多了三个零，故特殊处理
                    if column == "init_date":
                        interface_data /= 1000
                    if isinstance(sql_data[column_index], decimal.Decimal):
                        self.assertAlmostEqual(sql_data[column_index], round(decimal.Decimal(interface_data),4), msg=msg)
                        # self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result))
                    else:
                        self.assertEqual(sql_data[column_index], interface_data, msg=msg)
                except TypeError:
                    self.assertTrue(0, msg=msg)

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()


class ListMonthIntervalTradeAnalyze(unittest.TestCase):
    TABLE_NAME = "daily_asset"
    INTERFACE_NAME = "month_bill/list_month_interval_trade_analyze"
    COLUMNS = ["stock_count", "profit_count", "win_ratio"]

    @classmethod
    def setUpClass(cls):
        database_info = get_configurations.get_target_section(section='database_cnpsec')
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.conn = get_configurations.connect_to_mysql(database_info)
        cls.cur = cls.conn.cursor()

        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")
        # cls.login_info = interfaces.request(url=cls.url_prefix+"user/login", data='{"user_token":"888888"}',
                                            # is_get_method=False)

        print(cls.__class__.__name__, time.time())
        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + ListMonthIntervalTradeAnalyze.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # SQL
        sql = "SELECT stock_count,profit_count,win_ratio from month_interval_trade_analyze \
        where fund_account = {0} and interval_type = {1};".format(self.info["fund_account"], "5")
        print("Will execute sql: \n", sql)
        self.cur.execute(sql)
        sql_result = self.cur.fetchone()
        print("sql_result:\n", sql_result)
        sql_data = sql_result
        # 从 SQL 中获取的字段， 按照顺序比对每一个字段的值
        for column_index, column in enumerate(ListMonthIntervalTradeAnalyze.COLUMNS):
            try:
                interface_data = interface_result.get(column)
                msg="\nSQL is {0}\n Interface is {1}\n params is {2}\nCurrent is checking column {3}\n\
                Interface response is {4}".format(sql, url, data, column, interface_result)
                
                if isinstance(sql_data[column_index], decimal.Decimal):
                    self.assertAlmostEqual(sql_data[column_index], round(decimal.Decimal(interface_data),4), msg=msg)
                    # self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result))
                else:
                    self.assertEqual(sql_data[column_index], interface_data, msg=msg)
            except TypeError:
                self.assertTrue(0, msg=msg)

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()


if __name__ == "__main__":
    tests = unittest.TestSuite()
    tests.addTest(unittest.makeSuite(testCaseClass=GetMonthBillIncome, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=ListMonthBankChange, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=ListMonthIntervalTradeAnalyze, prefix='test'))
    with open(r"result_cnpsec.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

