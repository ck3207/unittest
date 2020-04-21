# -*- coding: utf-8 -*-
import unittest
import logging
import time, datetime
import decimal
import pymysql
import sys
import imp
imp.reload(sys)

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations
from business.interfaces import interfaces
from business.business import cumulative_rate
__author__ = "chenk"

class GetMonthBillIncome(unittest.TestCase):
    TABLE_NAME = "month_interval_asset_change"
    INTERFACE_NAME = "month_bill/get_month_bill_income"
    COLUMNS = ["asset_income", "asset_yield", "stock_income", "wit_income", "fund_income", 
    "other_income", "draw_back", "begin_asset", "last_asset", "fund_in", "fund_out", "fund_rank"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

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
        sql = sql_tmp[:-2] + " from {0} where fund_account = {1} and init_month = {2};"\
            .format(GetMonthBillIncome.TABLE_NAME, self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)
        cur.execute(sql)
        sql_result = cur.fetchone()
        _checking(self=self, class_name=GetMonthBillIncome, sql_result=sql_result, interface_result=interface_result,
                  is_fetchone=True, sql=sql, url=url, data=data)


class ListMonthBankChange(unittest.TestCase):
    TABLE_NAME = "daily_asset"
    INTERFACE_NAME = "month_bill/list_month_bank_change"
    COLUMNS = ["init_date", "asset", "fund_in", "fund_out"]
    
    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

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
        cur.execute(sql)
        sql_result = cur.fetchall()
        print("sql_result:\n", sql_result)
        _checking(self=self, class_name=ListMonthBankChange, sql_result=sql_result, interface_result=interface_result,
                  is_fetchone=False, sql=sql, url=url, data=data, is_timestamp=True)


class ListMonthIntervalTradeAnalyze(unittest.TestCase):
    TABLE_NAME = "month_interval_trade_analyze"
    INTERFACE_NAME = "month_bill/list_month_interval_trade_analyze"
    COLUMNS = ["stock_count", "profit_count", "win_ratio"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)

        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")
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
        where fund_account = {0} and init_month = {1};".format(self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)
        cur.execute(sql)
        sql_result = cur.fetchone()
        print("sql_result:\n", sql_result)
        _checking(self=self, class_name=ListMonthIntervalTradeAnalyze, sql_result=sql_result,
                  interface_result=interface_result, is_fetchone=True, sql=sql, url=url, data=data)


class ListMonthBankDetail(unittest.TestCase):
    TABLE_NAME = "month_bank_detail"
    INTERFACE_NAME = "month_bill/list_month_bank_detail"
    COLUMNS = ["part_init_date", "bank_name", "occ_price", "transfer_type"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + ListMonthBankDetail.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # SQL
        sql = "SELECT unix_timestamp(part_init_date) as part_init_date, bank_name, occ_price, transfer_type \
from month_bank_detail where fund_account = {0} and init_month = {1} \
order by part_init_date ;".format(self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)
        cur.execute(sql)
        sql_result = cur.fetchall()
        print("sql_result:\n", sql_result)
        _checking(self=self, class_name=ListMonthBankDetail, sql_result=sql_result, interface_result=interface_result,
                  is_fetchone=False, sql=sql, url=url, data=data, is_timestamp=True)


class ListMonthStockTradeStockCode(unittest.TestCase):
    TABLE_NAME = "month_stock_trade_detail"
    INTERFACE_NAME = "month_bill/list_month_stock_trade_stock_code"
    COLUMNS = ["stock_code", "stock_name"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + ListMonthStockTradeStockCode.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # SQL
        sql = "SELECT stock_code, stock_name from month_stock_trade_detail \
where fund_account = {0} and init_month = {1} group by stock_code, stock_name \
order by stock_code, stock_name limit 3;".format(self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)
        cur.execute(sql)
        sql_result = cur.fetchall()
        print("sql_result:\n", sql_result)
        _checking(self=self, class_name=ListMonthStockTradeStockCode, sql_result=sql_result,
                  interface_result=interface_result, is_fetchone=False,
                  sql=sql, url=url, data=data, count=len(sql_result))


class GetMonthStockTradeDetail(unittest.TestCase):
    TABLE_NAME = "month_stock_trade_detail"
    INTERFACE_NAME = "month_bill/get_month_stock_trade_detail"
    COLUMNS = ["init_date", "stock_code", "stock_name", "trade_num", "trade_price", "trade_state", "transaction_amount"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetMonthStockTradeDetail.INTERFACE_NAME
        data = self.data.copy()
        data.setdefault("page_no", 1)
        data.setdefault("page_size", self.info.get("page_size"))
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # SQL
        columns = GetMonthStockTradeDetail.COLUMNS.copy()
        columns.insert(columns.index("init_date"), "unix_timestamp({0})".format(columns.pop(columns.index("init_date"))))
        select_columns = sql_model.combine_select_columns(columns)
        sql = "SELECT {0} from month_stock_trade_detail where fund_account = {1} and init_month = {2} \
order by init_date, stock_code limit {3};".format(select_columns, self.info["fund_account"],
                                                        self.info["interval"], self.info["page_size"])
        sql_count = "SELECT {0} from month_stock_trade_detail where fund_account = {1} and init_month = {2} \
order by init_date, stock_code ;".format('count(1)', self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)

        try:
            cur.execute(sql)
            sql_result = cur.fetchall()
            cur.execute(sql_count)
            sql_count = cur.fetchone()
        except pymysql.err.ProgrammingError:
            self.assertTrue(0, msg="SQL Execute Error:\n{}".format(sql))
        print("sql_result:\n", sql_result)
        _checking(self=self, class_name=GetMonthStockTradeDetail, sql_result=sql_result,
                  interface_result=interface_result, is_fetchone=False,
                  sql=sql, url=url, data=data, count=sql_count[0], is_timestamp=True)


class GetMonthAccountYield(unittest.TestCase):
    INTERFACE_NAME = "month_bill/get_month_account_yield"
    COLUMNS = ["init_date", "income", "yield", "accumulative_yield", "position"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetMonthAccountYield.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # 删除首条数据
        interface_result.get("data_list").pop(0)
        # SQL
        columns = GetMonthAccountYield.COLUMNS.copy()
        columns.insert(columns.index("init_date"), "a.{0}".format(columns.pop(columns.index("init_date"))))
        columns.insert(columns.index("accumulative_yield"),
                       "0 as {0}".format(columns.pop(columns.index("accumulative_yield"))))
        select_columns = sql_model.combine_select_columns(columns)
#         sql = "SELECT {0} from daily_asset a INNER JOIN daily_index b on a.init_date = b.init_date where fund_account \
# = {1} and b.init_date >= (SELECT max(init_date) as init_date from daily_index where init_date < '{2}01') and \
# b.init_date <= '{2}31' ORDER BY init_date;".format(select_columns, self.info["fund_account"], self.info["interval"])
        sql = "SELECT init_date, income, income / get_last_trading_day_asset(init_date, {0}) as day_yield,  \
get_month_total_income(init_date, {0})/get_max_asset(init_date, {0}) as accumulative_yield from daily_asset \
where fund_account = {0} and substr(init_date from 1 for 6) = {1}  ORDER BY init_date;"\
            .format(self.info["fund_account"], self.info["interval"])
        print("Will execute sql: \n", sql)

        try:
            cur.execute(sql)
            sql_result = cur.fetchall()
            rate_index = {}
            rate_index.setdefault(GetMonthAccountYield.COLUMNS.index("yield"),
                                  GetMonthAccountYield.COLUMNS.index("accumulative_yield"))
            sql_result_dealed = sql_result
            # sql_result_dealed = cumulative_rate.cal_cumulative_rate(data=sql_result,
            #                                                         rate_indexes=rate_index,
            #                                                         need_to_deal_first_data=True,
            #                                                         GetMonthAccountYield=True)
        except pymysql.err.ProgrammingError:
            self.assertTrue(0, msg="SQL Execute Error:\n{}".format(sql))
        # self.assertTrue(0, msg="SQL:\n{0}\nResponse:\n{1}".format(sql, sql_result_dealed))
        print("sql_result:\n", sql_result_dealed)
        _checking(self=self, class_name=GetMonthAccountYield, sql_result=sql_result_dealed,
                  interface_result=interface_result, is_fetchone=False,
                  sql=sql, url=url, data=data, count=len(sql_result_dealed)+1, is_timestamp=False)


class GetMonthIndexAcYield(unittest.TestCase):
    INTERFACE_NAME = "month_bill/get_month_index_ac_yield"
    COLUMNS = ["init_date", "hs_yield", "sh_yield", "sz_yield", "gem_yield", "ac_hs_yield", "ac_sh_yield",
               "ac_sz_yield", "ac_gem_yield", "hs_income", "sh_income", "sz_income", "gem_income"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='cnpsec_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_cnpsec_prefix")

        cls.data = {}
        cls.data.setdefault("interval", cls.info.get("interval"))
        cls.data.setdefault("fund_account", cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetMonthIndexAcYield.INTERFACE_NAME
        data = self.data.copy()
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # columns should be transferred which is not just selected.
        columns = GetMonthIndexAcYield.COLUMNS.copy()
        columns.insert(columns.index("ac_hs_yield"), "0 as {0}".format(columns.pop(columns.index("ac_hs_yield"))))
        columns.insert(columns.index("ac_sh_yield"), "0 as {0}".format(columns.pop(columns.index("ac_sh_yield"))))
        columns.insert(columns.index("ac_sz_yield"), "0 as {0}".format(columns.pop(columns.index("ac_sz_yield"))))
        columns.insert(columns.index("ac_gem_yield"), "0 as {0}".format(columns.pop(columns.index("ac_gem_yield"))))
        columns.insert(columns.index("hs_income"), "0 as {0}".format(columns.pop(columns.index("hs_income"))))
        columns.insert(columns.index("sh_income"), "0 as {0}".format(columns.pop(columns.index("sh_income"))))
        columns.insert(columns.index("sz_income"), "0 as {0}".format(columns.pop(columns.index("sz_income"))))
        columns.insert(columns.index("gem_income"), "0 as {0}".format(columns.pop(columns.index("gem_income"))))

        select_columns = sql_model.combine_select_columns(columns)
        sql = "SELECT {0} from daily_index where init_date BETWEEN '{1}01' and '{1}31' \
order by init_date;".format(select_columns, self.info["interval"])
        print("Will execute sql: \n", sql)

        try:
            cur.execute(sql)
            sql_result = cur.fetchall()
            rate_index = {}
            rate_index.setdefault(GetMonthIndexAcYield.COLUMNS.index("hs_yield"),
                                  GetMonthIndexAcYield.COLUMNS.index("ac_hs_yield"))
            rate_index.setdefault(GetMonthIndexAcYield.COLUMNS.index("sh_yield"),
                                  GetMonthIndexAcYield.COLUMNS.index("ac_sh_yield"))
            rate_index.setdefault(GetMonthIndexAcYield.COLUMNS.index("sz_yield"),
                                  GetMonthIndexAcYield.COLUMNS.index("ac_sz_yield"))
            rate_index.setdefault(GetMonthIndexAcYield.COLUMNS.index("gem_yield"),
                                  GetMonthIndexAcYield.COLUMNS.index("ac_gem_yield"))
            sql_result_dealed = cumulative_rate.cal_cumulative_rate(data=sql_result,
                                                                    rate_indexes=rate_index,
                                                                    need_to_deal_first_data=False,
                                                                    GetMonthIndexAcYield=True)

        except pymysql.err.ProgrammingError:
            self.assertTrue(0, msg="SQL Execute Error:\n{}".format(sql))
        # self.assertTrue(0, msg="SQL:\n{0}\nResponse:\n{1}".format(sql, sql_result_dealed))
        print("sql_result:\n", sql_result_dealed)
        _checking(self=self, class_name=GetMonthIndexAcYield, sql_result=sql_result_dealed,
                  interface_result=interface_result, is_fetchone=False, sql=sql, url=url, data=data,
                  count=len(sql_result_dealed), is_timestamp=False, is_cumulative_rate=True)


class GetCursor:
    def __init__(self, section):
        self.database_info = get_configurations.get_target_section(section=section)
        self.conn, self.cur = "", ""

    def get_cursor(self):
        if not self.cur:
            return self.__create_cursor()
        else:
            return self.cur

    def __create_cursor(self):
        self.conn = get_configurations.connect_to_mysql(self.database_info)
        self.cur = self.conn.cursor()
        return self.cur

    def close_connection(self):
        self.cur.close()
        self.conn.close()
        print("Close database connection successully.")


def _checking(self, class_name, sql_result, interface_result, is_fetchone=True, **params):
    """
    
    :param self: unittest object
    :param class_name: a test class name
    :param sql_result: result which executed in SQL
    :param interface_result: result which returned in interface
    :param is_fetchone: whether the result get via function fetchone or not
    :param params: sql which current test executed. 
    url which current test requested. 
    data is the params in interface requesting.
    count is the num of return list.
    :return: 
    """
    # 统一日志信息
    msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
    SQL result is\n{4}".format(params.get("sql"), params.get("url"), params.get("data"), interface_result, sql_result)
    # SQL 执行结果为一条数据时，对比每一条记录里面的字段值
    if is_fetchone:
        for i, column in enumerate(class_name.COLUMNS):
            try:
                if isinstance(sql_result[i], decimal.Decimal):
                    self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)), msg=msg_model)
                else:
                    self.assertEqual(sql_result[i], interface_result.get(column), msg=msg_model)
            except TypeError:
                self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                Current is checking column {3}".format(params.get("sql"), params.get("url"), params.get("data"), column))

    else:
        # SQL 执行结果为多条数据时，需取每一条数据，对比每一条记录里面的字段值
        if params.get("count"):
            self.assertEqual(params.get("count"), interface_result.get("count"), msg=msg_model)
        for sql_index, sql_data in enumerate(sql_result):
            # 从 SQL 中获取的字段， 按照顺序比对每一个字段的值
            for column_index, column in enumerate(class_name.COLUMNS):
                try:
                    interface_data = interface_result.get("data_list")[sql_index].get(column)
                    msg = msg_model + "\nCurrent is checking column {0}\n".format(column)
                    # 该字段为时间戳， 且接口时间戳字段比 SQL 查询的时间戳字段 多了三个零，故特殊处理
                    if params.get("is_timestamp") and column in ["init_date", "part_init_date"]:
                        interface_data /= 1000
                    if isinstance(sql_data[column_index], decimal.Decimal):
                        # 累计收益率由于保留小数问题，存在一定误差
                        if params.get("is_cumulative_rate"):
                            self.assertTrue(abs(sql_data[column_index] - decimal.Decimal(interface_data)) <= 0.0015,
                                            msg=msg)
                        else:
                            self.assertAlmostEqual(sql_data[column_index], round(decimal.Decimal(interface_data), 4), msg=msg)
                    else:
                        # self.assertEqual(sql_data[column_index], interface_data, msg="sql_data:{0},interface_data:{1},\n\
                        # column_index:{2}".format(sql_data, interface_result.get("data_list")[sql_index], column_index))
                        self.assertEqual(sql_data[column_index], interface_data, msg=msg)
                except TypeError:
                    self.assertTrue(0, msg=msg)

class SQLModel:
    def __init__(self):
        self.sql_model = ""

    def set_sql_model(self, sql_model, select_columns, fileter_conditons, order_by, group_by):
        {"select_columns": "column1, column2",
         "filter_conditions": {"fund_account": "88888", "init_date": "20190501"},
         "order_by": "order by init_date asc, stock_code asc",
         "group_by": "group_by by init_date asc, stock_code asc"}
        self.sql_model = ""

    def get_sql_model(self):
        pass

    def combine_select_columns(self, columns_list):
        sql = ""
        for column in columns_list:
            sql += column + ", "
        return sql[:-2]


if __name__ == "__main__":
    # create database connection
    get_cursor = GetCursor(section='database_cnpsec')
    cur = get_cursor.get_cursor()

    # combine sql
    sql_model = SQLModel()

    # create unittest tests
    tests = unittest.TestSuite()
    tests.addTest(unittest.makeSuite(testCaseClass=GetMonthBillIncome, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=ListMonthBankChange, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=ListMonthIntervalTradeAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=ListMonthBankDetail, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=ListMonthStockTradeStockCode, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetMonthStockTradeDetail, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetMonthAccountYield, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetMonthIndexAcYield, prefix='test'))

    # write unittest result to a file
    with open(r"result_cnpsec.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

    # close database connection
    get_cursor.close_connection()
