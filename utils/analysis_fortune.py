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
                 sql=hbase_command, url=url, data=data)


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
    :return: 
    """
    # 统一日志信息
    msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
    Hbase result is\n{4}".format(params.get("sql"), params.get("url"), params.get("data"), interface_result, sql_result)
    # SQL 执行结果为一条数据时，对比每一条记录里面的字段值
    if params.get("is_hbase_result"):
        sql_result = hbase_result_deal.deal(sql_result, is_json_content=params.get("is_json_content"),
                                            list_name="bond_hold_data_list")
        try:
            for column in GetPageUserDailyData.COLUMNS:
                if not column.endswith("_list"):
                    self.assertEqual(str(sql_result.get(column)), str(interface_result.get(column)), msg=msg_model)
                else:
                    self.assertEqual(sql_result.get(column), interface_result.get(column), msg=msg_model)
        except TypeError:
            self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                            Current is checking column {3}".format(params.get("sql"), params.get("url"),
                                                                   params.get("data"), column))
        # for i, column in enumerate(class_name.COLUMNS):
        #     try:
        #         if isinstance(sql_result[i], decimal.Decimal):
        #             self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)), msg=msg_model)
        #         else:
        #             self.assertEqual(sql_result[i], interface_result.get(column), msg=msg_model)
        #     except TypeError:
        #         self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
        #         Current is checking column {3}".format(params.get("sql"), params.get("url"), params.get("data"), column))

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
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthBankChange, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthIntervalTradeAnalyze, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthBankDetail, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthStockTradeStockCode, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthStockTradeDetail, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthAccountYield, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthIndexAcYield, prefix='test'))

    # write unittest result to a file
    with open(r"result_chasing.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

