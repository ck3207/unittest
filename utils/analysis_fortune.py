# -*- coding: utf-8 -*-
import unittest
import time, datetime
import decimal
import sys
import imp
imp.reload(sys)

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations, hbase_client
from business.interfaces import interfaces
from business.business import cumulative_rate, get_month_account_yield, hbase_result_deal
__author__ = "chenk"


class GetPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "bond_page_user_daily_data"])
    INTERFACE_NAME = "general/get_bond_page_user_daily_data"
    COLUMNS = ["init_date", "bond_asset", "bond_daily_income", "json_content", "bond_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetPageUserDailyData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetPageUserDailyData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetPageUserDailyData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetPageUserDailyData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetPageUserDailyData.COLUMNS)


class GetHomePageUserIntervalData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "home_page_user_interval_data"])
    INTERFACE_NAME = "general/get_home_page_user_interval_data"
    COLUMNS = ["asset_income", "asset_yield", "stock_income", "financial_income", "cfb_income",
               "bond_income", "begin_asset", "fund_in", "fund_out", "last_asset", "sharpe",
               "draw_down", "income_loss_per", "income_risk_per"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"),
                                        interval=cls.info.get("interval"), asset_prop=cls.info.get("asset_prop"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetHomePageUserIntervalData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetHomePageUserIntervalData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetHomePageUserIntervalData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetHomePageUserIntervalData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetHomePageUserIntervalData.COLUMNS)


class GetHomePageUserCurveData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "home_page_user_daily_data"])
    INTERFACE_NAME = "general/get_home_page_user_curve_data"
    COLUMNS = ["monthDataList"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"),
                                        fund_account=cls.info.get("fund_account"), interval=cls.info.get("interval"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetHomePageUserCurveData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetHomePageUserCurveData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetHomePageUserCurveData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetHomePageUserCurveData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data)


class GetBondPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "bond_page_user_daily_data"])
    INTERFACE_NAME = "general/get_bond_page_user_daily_data"
    COLUMNS = ["init_date", "bond_asset", "bond_daily_income", "json_content", "bond_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetBondPageUserDailyData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        if self.data.get("interval"):
            row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("fund_account_reversed"),
                                init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetBondPageUserDailyData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetBondPageUserDailyData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetBondPageUserDailyData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, list_name=["bond_hold_data", "bond_hold_data_list"],
                 table_columns=GetBondPageUserDailyData.COLUMNS, special_column=["init_date"],
                 deal_column=["bond_nums", int])


class GetFinancialPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "financial_page_user_daily_data"])
    INTERFACE_NAME = "general/get_financial_page_user_daily_data"
    COLUMNS = ["init_date", "financial_asset", "financial_daily_income", "financial_hold_data",
               "financial_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetFinancialPageUserDailyData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        if self.data.get("interval"):
            row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("fund_account_reversed"),
                                init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetFinancialPageUserDailyData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetFinancialPageUserDailyData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetFinancialPageUserDailyData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, list_name=["financial_hold_data", "financial_hold_data_list"],
                 table_columns=GetFinancialPageUserDailyData.COLUMNS)


class GetFinancialPageUserIntervalData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "financial_page_user_interval_data"])
    INTERFACE_NAME = "general/get_financial_page_user_interval_data"
    COLUMNS = ["financial_income_balance", "financial_hold_num", "financial_win_num", "most_profit_financial_code",
               "most_profit_financial_name", "most_profit_financial_income", "most_loss_financial_code",
               "most_loss_financial_name", "most_loss_financial_income", "financial_buy_num", "financial_sell_num",
               "json_content", "financial_trade_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"),
                                        fund_account=cls.info.get("fund_account"),
                                        interval=cls.info.get("interval"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetFinancialPageUserIntervalData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetFinancialPageUserIntervalData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetFinancialPageUserIntervalData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetFinancialPageUserIntervalData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetFinancialPageUserIntervalData.COLUMNS,
                 list_name=["financial_trade_data", "financial_trade_data_list"], deal_column=["hold_days", int])


class GetStockPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_chasing').get("database_prefix"),
                          "stock_page_user_daily_data"])
    INTERFACE_NAME = "general/get_stock_page_user_daily_data"
    COLUMNS = ["init_date", "stock_asset", "stock_daily_income", "stock_daily_income_ratio",
               "stock_hs_asset", "stock_hk_asset", "infund_asset", "stock_day_position",
               "stock_hold_data", "stock_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='chasing_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_chasing_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetStockPageUserDailyData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        if self.data.get("interval"):
            row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("fund_account_reversed"),
                                init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetStockPageUserDailyData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetStockPageUserDailyData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetStockPageUserDailyData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, list_name=["stock_hold_data", "stock_hold_data_list"],
                 table_columns=GetStockPageUserDailyData.COLUMNS, special_column=["stock_market_value", "cost_price"],
                 deal_column=["hold_amount", int])


def checking(self, class_name, sql_result, interface_result, is_fetchone=True, **params):
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
    list_name: is a list name of response. 
    table_columns:  is a list like ["init_date", "bond_num"]. The column in list need to be checking.
    special_column is a list like ["int_date", "bond_num"]. The column in list need to be filter when checking.
    deal_column is a list like ["bond_num", int]. The fisrt index is the name of column, 
    the second is the function which column need to be dealed specially.
    :return: 
    """
    # 统一日志信息
    msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
    Hbase result is\n{4}".format(params.get("sql"), params.get("url"), params.get("data"), interface_result, sql_result)
    # SQL 执行结果为一条数据时，对比每一条记录里面的字段值
    if params.get("is_hbase_result"):
        sql_result = hbase_result_deal.deal(sql_result, is_json_content=params.get("is_json_content"),
                                            list_name=params.get("list_name"))
        try:
            for column in params.get("table_columns"):
                if not column.lower().endswith("list"):
                    self.assertEqual(str(sql_result.get(column)), str(interface_result.get(column)), msg=msg_model)
                else:
                    for i, info in enumerate(sql_result.get(column)):
                        for k, v in info.items():
                            deal_column = params.get("deal_column", [False])
                            # 需要过滤的字段，即无需验证字段
                            if k in params.get("special_column", []):
                                continue
                            # 需要验证的字段 且 不需要特殊处理的字段
                            if k not in params.get("special_column", []) and k != deal_column[0]:
                                self.assertEqual(str(v), str(interface_result.get(column)[i].get(k)), msg=msg_model)
                            # 需要特殊处理的字段
                            elif deal_column and k == deal_column[0]:
                                self.assertEqual(str(deal_column[1](v)), str(interface_result.get(column)[i].get(k)),
                                                 msg=msg_model)
                            # 其他
                            else:
                                self.assertEqual(str(v), str(interface_result.get(column)[i].get(k)), msg=msg_model)
        except TypeError:
            self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                            Current is checking column {3}".format(params.get("sql"), params.get("url"),
                                                                   params.get("data"), column))

    else:
        if is_fetchone:
            for i, column in enumerate(class_name.COLUMNS):
                try:
                    if isinstance(sql_result[i], decimal.Decimal):
                        self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)), msg=msg_model)
                    else:
                        self.assertEqual(sql_result[i], interface_result.get(column), msg=msg_model)
                except TypeError:
                    self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                    Current is checking column {3}".format(params.get("sql"), params.get("url"), params.get("data"),
                                                           column))

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
                                self.assertAlmostEqual(sql_data[column_index], round(decimal.Decimal(interface_data), 4),
                                                       msg=msg)
                        else:
                            # self.assertEqual(sql_data[column_index], interface_data, msg="sql_data:{0},interface_data:{1},\n\
                            # column_index:{2}".format(sql_data, interface_result.get("data_list")[sql_index], column_index))
                            self.assertEqual(sql_data[column_index], interface_data, msg=msg)
                    except TypeError:
                        self.assertTrue(0, msg=msg)


def init_date_to_cal_date(init_date):
    """hbase主键中的cal_date 需特殊处理——9个9减去日期"""
    return str(99999999 - int(init_date))


def get_basic_paramaters(**params):
    """根据传参，设置参数"""
    params_dic = {}
    for param, value in params.items():
        params_dic.setdefault(param, value)
    if "fund_account" in params_dic.keys():
        params_dic.setdefault("fund_account_reversed", "".join(reversed(params.get("fund_account"))))
    return params_dic

if __name__ == "__main__":
    # create unittest tests
    tests = unittest.TestSuite()
    tests.addTest(unittest.makeSuite(testCaseClass=GetPageUserDailyData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetHomePageUserIntervalData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetHomePageUserCurveData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetFinancialPageUserIntervalData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetBondPageUserDailyData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetFinancialPageUserDailyData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetStockPageUserDailyData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthIntervalTradeAnalyze, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthBankDetail, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthStockTradeStockCode, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthStockTradeDetail, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthAccountYield, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthIndexAcYield, prefix='test'))

    # write unittest result to a file
    with open(r"result_chasing.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

