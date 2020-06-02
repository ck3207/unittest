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
from business.business import cumulative_rate, get_month_account_yield, hbase_result_deal, special_date
__author__ = "chenk"


class GetAccountYield(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "user_daily_data"])
    INTERFACE_NAME = "general/get_account_yield"
    COLUMNS = ["init_date", "income", "yield", "accumulative_yield", "position",
               "daily_income", "daily_income_ratio"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"),
                                        interval=cls.info.get("interval"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetAccountYield.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetAccountYield.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetAccountYield.TABLE_NAME, row_key)

        checking(self=self, class_name=GetAccountYield, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetAccountYield.COLUMNS,
                 special_column=["accumulative_yield"])


class GetIncomeAnalyze(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "general/get_income_analyze"
    COLUMNS = ['fund_out', 'fund_in', 'begin_asset', 'last_asset', 'net_inflow', 'asset_income', 'asset_yield',
               'stock_income', 'wit_income', 'fund_income', 'other_income', 'draw_back', 'fund_rank', 'bond_income',
               'otc_income', 'other_assets_income', 'begin_date', 'end_date', 'bshare_income', 'net_inflow']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"),
                                        interval=cls.info.get("interval"), asset_prop=cls.info.get("asset_prop"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetIncomeAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetIncomeAnalyze.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetIncomeAnalyze.TABLE_NAME, row_key)

        checking(self=self, class_name=GetIncomeAnalyze, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetIncomeAnalyze.COLUMNS,
                 calculation=["net_inflow", "fund_in", "-", "fund_out"],
                 list_name=["assure_yield", "asset_yield", "bond_income", "wit_income", "otc_income", "fund_income",
                            "other_assets_income", "other_income"])


class GetInvestAnalyze(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_user_month_data"])
    INTERFACE_NAME = "general/get_invest_analyze"
    COLUMNS = ["my_ability", "avg_ability", "ability_rank"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"),
                                        fund_account=cls.info.get("fund_account"), interval=cls.info.get("interval"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetHomePageUserCurveData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)

        # monthDataList
        hbase_result_deal_list = []
        hbase_result = {}
        for month in special_date.get_init_month(cal_init_date=self.info.get("cal_init_date")):
            row_key = ",".join([self.data.get("fund_account_reversed"), month])
            # hbase_command = """get "{0}", "{1}" """.format(GetHomePageUserCurveData.TABLE_NAME, row_key)
            # self.assertTrue(0, msg="{0}".format(hbase_command))
            hbase_result_origin = hbase_client.getRow(tableName=GetHomePageUserCurveData.TABLE_NAME, row=row_key)
            hbase_result_deal_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                            init_month=month,
                                                                            month_income="month_income")
            if len(hbase_result_deal_dict) > 0:
                hbase_result_deal_list.append(hbase_result_deal_dict)

        hbase_result.setdefault("monthDataList", hbase_result_deal_list)

        # homeCurveDataList
        hbase_result_deal_list = []
        init_date_base = special_date.get_init_date(self.info.get("init_date"), self.info.get("interval"))
        for i in range(30):
            init_date = special_date.get_date(init_date_base, i)
            row_key = ",".join([self.data.get("fund_account_reversed"),
                                init_date_to_cal_date(init_date)])
            if not locals().get("init_total_asset"):
                init_total_asset_row_key = ",".join([self.data.get("fund_account_reversed"),
                                             init_date_to_cal_date(special_date.get_date(init_date_base, -1))])
                hbase_total_asset = hbase_client.get(tableName="chenk_zhfx:home_page_user_daily_data",
                                                       row=init_total_asset_row_key, column="tag_base:total_asset")
                init_total_asset = eval(hbase_total_asset[0].value)
            # hbase_command = """get "{0}", "{1}" """.format(GetHomePageUserCurveData.TABLE_NAME, row_key)
            # self.assertTrue(0, msg="{0}".format(hbase_command))
            hbase_result_origin = hbase_client.getRow(tableName="chenk_zhfx:home_page_user_daily_data", row=row_key)
            hbase_result_deal_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                            init_date=init_date,
                                                                            total_asset="total_asset",
                                                                            daily_income="daily_income",
                                                                            daily_income_ratio="daily_income_ratio",
                                                                            ac_daily_income_ratio="ac_daily_income_ratio",
                                                                            fund_in="fund_in",
                                                                            fund_out="fund_out")
            if len(hbase_result_deal_dict) > 0:
                hbase_result_deal_list.append(hbase_result_deal_dict)

        hbase_result_deal_list = self.__set_cumulative_ratio(init_total_asset, hbase_result_deal_list,
                                                             "ac_daily_income_ratio")
        hbase_result.setdefault("homeCurveDataList", hbase_result_deal_list)
        # self.assertTrue(0, msg="{0}--".format(hbase_result))
        hbase_command = """get "{0}", "{1}" """.format(GetHomePageUserCurveData.TABLE_NAME, row_key)
        checking(self=self, class_name=GetHomePageUserCurveData, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetHomePageUserCurveData.COLUMNS,
                 list_name=["monthDataList", "monthDataList", "homeCurveDataList", "homeCurveDataList"])

    def __set_cumulative_ratio(self, cost, data_list, key):
        tmp_cost = cost
        cost = cost  # 成本
        balance = 0
        new_data_list = []
        for i, data in enumerate(data_list):
            balance += eval(data.get("daily_income"))
            fund_in = eval(data.get("fund_in")) - eval(data.get("fund_out"))
            tmp_cost += fund_in
            if tmp_cost > cost:
                cost = tmp_cost
            ac_daily_income_ratio = round(balance / cost, 6)
            data.setdefault(key, ac_daily_income_ratio)
            new_data_list.append(data)

        return new_data_list


class GetLastAsset(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "user_daily_asset"]) # 原表 cash_page_user_daily_data 改成 home_page_user_daily_data
    INTERFACE_NAME = "general/get_last_asset"
    COLUMNS = ["asset", "init_date", "is_last", "begin_RMB_asset", "end_RMB_asset", "begin_HKD_asset",
               "end_HKD_asset", "begin_doller_asset", "end_doller_asset"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetLastAsset.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)


        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])

        hbase_result_origin = hbase_client.getRow(tableName=GetLastAsset.TABLE_NAME, row=row_key)
        # self.assertTrue(0, msg="{0}--".format(hbase_result))
        hbase_command = """get "{0}", "{1}" """.format(GetLastAsset.TABLE_NAME, row_key)
        checking(self=self, class_name=GetLastAsset, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetLastAsset.COLUMNS,
                 special_column=["is_last"])


class GetCfbPageUserCurveData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "cfb_page_user_daily_data"])
    INTERFACE_NAME = "general/get_cfb_page_user_curve_data"
    COLUMNS = ["cfbCurveDataList"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"),
                                        fund_account=cls.info.get("fund_account"), interval=cls.info.get("interval"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetCfbPageUserCurveData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)

        # cashCurveDataList
        hbase_result = {}
        hbase_result_deal_list = []
        init_date_base = special_date.get_init_date(self.info.get("init_date"), self.info.get("interval"))
        for i in range(30):
            init_date = special_date.get_date(init_date_base, i)
            row_key = ",".join([self.data.get("fund_account_reversed"),
                                init_date_to_cal_date(init_date)])

            hbase_result_origin = hbase_client.getRow(tableName=GetCfbPageUserCurveData.TABLE_NAME, row=row_key)
            hbase_result_deal_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                            init_date=init_date,
                                                                            cfb_asset="cfb_asset",
                                                                            cfb_daily_income="cfb_daily_income")
            if len(hbase_result_deal_dict) > 0:
                hbase_result_deal_list.append(hbase_result_deal_dict)

        hbase_result.setdefault("cfbCurveDataList", hbase_result_deal_list)
        # self.assertTrue(0, msg="{0}--".format(hbase_result))
        hbase_command = """get "{0}", "{1}" """.format(GetCfbPageUserCurveData.TABLE_NAME, row_key)
        checking(self=self, class_name=GetCfbPageUserCurveData, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetCfbPageUserCurveData.COLUMNS,
                 list_name=["cfbCurveDataList", "cfbCurveDataList"])


class GetBondPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "bond_page_user_daily_data"])
    INTERFACE_NAME = "general/get_bond_page_user_daily_data"
    COLUMNS = ["init_date", "bond_asset", "bond_daily_income", "bond_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(init_date=cls.info.get("init_date"), fund_account=cls.info.get("fund_account"))

    def test_normal(self):
        """"""
        url = self.url_prefix + GetBondPageUserDailyData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetBondPageUserDailyData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetBondPageUserDailyData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetBondPageUserDailyData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, list_name=["json_content", "bond_hold_data_list"],
                 table_columns=GetBondPageUserDailyData.COLUMNS, special_column=["init_date"],
                 deal_column=["bond_nums", int])


class GetBondPageUserIntervalData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "bond_page_user_interval_data"])
    INTERFACE_NAME = "general/get_bond_page_user_interval_data"
    COLUMNS = ["bond_income", "bond_hold_num", "bond_win_num", "bond_win_rate", "most_profit_bond_code",
               "most_profit_bond_name", "most_profit_bond_income", "most_loss_bond_code",
               "most_loss_bond_name", "most_loss_bond_income", "bond_business_balance", "bond_buy_times",
               "bond_sell_times", "bond_borrow_times", "bond_lend_times", "bond_new_times", "bond_trade_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetBondPageUserIntervalData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetBondPageUserIntervalData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetBondPageUserIntervalData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetBondPageUserIntervalData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, list_name=["json_content", "bond_trade_data_list"],
                 table_columns=GetBondPageUserIntervalData.COLUMNS, special_column=["init_date", "exchange_type"],
                 deal_column=["hold_days", int])


class GetFinancialPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "financial_page_user_daily_data"])
    INTERFACE_NAME = "general/get_financial_page_user_daily_data"
    COLUMNS = ["init_date", "financial_asset", "financial_daily_income", "financial_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
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
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "financial_page_user_interval_data"])
    INTERFACE_NAME = "general/get_financial_page_user_interval_data"
    COLUMNS = ["financial_income_balance", "financial_hold_num", "financial_win_num", "most_profit_financial_code",
               "most_profit_financial_name", "most_profit_financial_income", "most_loss_financial_code",
               "most_loss_financial_name", "most_loss_financial_income", "financial_buy_num", "financial_sell_num",
               "financial_trade_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
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
                 list_name=["json_content", "financial_trade_data_list"], deal_column=["hold_days", int])


class GetStockPageUserDailyData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "stock_page_user_daily_data"])
    INTERFACE_NAME = "general/get_stock_page_user_daily_data"
    COLUMNS = ["init_date", "stock_asset", "stock_daily_income", "stock_daily_income_ratio",
               "stock_hs_asset", "stock_hk_asset", "infund_asset", "stock_day_position", "stock_hold_data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
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


class GetStockPageUserIntervalData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "stock_page_user_interval_data"])
    INTERFACE_NAME = "general/get_stock_page_user_interval_data"
    COLUMNS = ['stock_income', 'stock_hold_num', 'stock_win_num', 'stock_win_rate', 'most_profit_stock_code',
               'most_profit_stock_name', 'most_profit_stock_income', 'most_loss_stock_code', 'most_loss_stock_name',
               'most_loss_stock_income', 'stock_avg_position', 'stock_profit_sell_rate', 'stock_profit_sell_times',
               'stock_loss_sell_times', 'stock_buy_times', 'stock_sell_times', 'stock_trade_times',
               'stock_avg_hold_days', 'stock_allsell_times', 'stock_new_times', 'fund_buy_balance',
               'fund_sell_balance', 'fund_buy_times', 'fund_sell_times', 'trade_rate', 'trade_stock_count']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetStockPageUserIntervalData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.info.get("interval"),
                            self.info.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])

        hbase_result_origin = hbase_client.getRow(tableName=GetStockPageUserIntervalData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetStockPageUserIntervalData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetStockPageUserIntervalData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetStockPageUserIntervalData.COLUMNS,
                 special_column=["stock_trade_times"])


class GetStockDetailPageData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "stock_detail_page_data"])
    INTERFACE_NAME = "general/get_stock_detail_page_data"
    COLUMNS = ['stock_data_list', 'bond_data_list']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetStockDetailPageData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(self.info.get("init_date")),
                            self.info.get("stock_code"), self.info.get("exchange_type")])

        hbase_result_origin = hbase_client.getRow(tableName=GetStockDetailPageData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetStockDetailPageData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetStockDetailPageData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetStockDetailPageData.COLUMNS,
                 list_name=["json_content", "stock_data_list"], deal_column=["business_amount", int])


class GetNewStockPageUserIntervalData(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "new_stock_page_data"])
    INTERFACE_NAME = "general/get_new_stock_page_user_interval_data"
    COLUMNS = ['income_balance', 'stock_data_list']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetNewStockPageUserIntervalData.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.info.get("interval"),
                            self.info.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])

        hbase_result_origin = hbase_client.getRow(tableName=GetNewStockPageUserIntervalData.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetNewStockPageUserIntervalData.TABLE_NAME, row_key)

        checking(self=self, class_name=GetNewStockPageUserIntervalData, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetNewStockPageUserIntervalData.COLUMNS,
                 list_name=["stock_content", "stock_data_list", "bond_content", "bond_data_list"],
                 deal_column=["business_amount", int])


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
    list_name: is a list name of response. The index zero is the old colunm name which is in hbase database,
            the index one is the new column name which is in the response of interface.
    table_columns:  is a list like ["init_date", "bond_num"]. The column in list need to be checking.
    special_column is a list like ["int_date", "bond_num"]. The column in list need to be filter when checking.
    deal_column is a list like ["bond_num", int]. The fisrt index is the name of column, 
    the second is the function which column need to be dealed specially.
    calculation is a list like ["new_column", "column1", "+", "column2"]. The first one is the new column which will
        returned in interface. The others can be joined together as a arithmetic expressions. 
    :return: 
    """
    # 统一日志信息
    msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
    Hbase result is\n{4}".format(params.get("sql"), params.get("url"), params.get("data"), interface_result, sql_result)
    # SQL 执行结果为一条数据时，对比每一条记录里面的字段值
    if params.get("is_hbase_result"):
        sql_result = hbase_result_deal.deal(sql_result, is_json_content=params.get("is_json_content"),
                                            list_name=params.get("list_name"))

        # 根据传入的数据进行计算， 列表中第一个为输出的字段，其他个数为计算表达式的一部分
        if params.get("calculation"):
            expression = ""
            new_column = params.get("calculation")[0]
            for i, each in enumerate(params.get("calculation")[1:]):
                if i % 2 == 0:
                    expression += sql_result.get(each)
                else:
                    expression += each
            sql_result.setdefault(new_column, round(eval(expression), 4))

        # 处理接口输出字段与hbase查询出的输出字段不一致的情况
        if len(params.get("list_name", [])) > 1:
            for column in params.get("list_name"):
                if not sql_result.get(column, False):
                    origin_column = column
                    # list_name 允许存在多组数据， eg: [old_1, new_1, old_2, new_2]
                    if params.get("list_name").index(column) / 2 == 0:
                        tmp = params.get("list_name").index(column) + 1
                    else:
                        tmp = params.get("list_name").index(column) - 1
                    new_column = params.get("list_name")[tmp]
                    sql_result.setdefault(new_column, sql_result.get(origin_column))

        # params.get("cal_column")
        # self.assertTrue(0, msg="new sql_result: {0}".format(sql_result))
        try:
            for column in params.get("table_columns"):
                if column not in params.get("list_name", []):
                    if column not in params.get("special_column", []):
                        self.assertEqual(str(sql_result.get(column)), str(interface_result.get(column)), msg=msg_model)
                        continue
                elif isinstance(sql_result.get(column), list):
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
            if not locals().get(column):
                column = "cant get."
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


def get_basic_paramaters(option_info={}, **params):
    """根据传参，设置参数"""
    params_dic = {}
    stock_trade_times = params.get("stock_trade_times", False)
    for option_name, option_value in option_info.items():
        params_dic.setdefault(option_name, option_value)

    for param, value in params.items():
        params_dic.setdefault(param, value)
    if "fund_account" in params_dic.keys():
        params_dic.setdefault("fund_account_reversed", "".join(reversed(params_dic.get("fund_account"))))
    if stock_trade_times:
        params_dic.setdefault("stock_trade_times",
                              params_dic.get(stock_trade_times[0] + params_dic.get(stock_trade_times[1])))
    return params_dic

if __name__ == "__main__":
    # create unittest tests
    tests = unittest.TestSuite()
    tests.addTest(unittest.makeSuite(testCaseClass=GetAccountYield, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetIncomeAnalyze, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetHomePageUserCurveData, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetLastAsset, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetCfbPageUserCurveData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetFinancialPageUserIntervalData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetBondPageUserDailyData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetBondPageUserIntervalData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetFinancialPageUserDailyData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetStockPageUserDailyData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetStockPageUserIntervalData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetStockDetailPageData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetNewStockPageUserIntervalData, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=ListMonthStockTradeStockCode, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthStockTradeDetail, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthAccountYield, prefix='test'))
    # tests.addTest(unittest.makeSuite(testCaseClass=GetMonthIndexAcYield, prefix='test'))

    # write unittest result to a file
    with open(r"../export/result_guolian.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

