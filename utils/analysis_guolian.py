# -*- coding: utf-8 -*-
import unittest
import time, datetime
import keyword
import decimal
import sys
# import imp
# imp.reload(sys)

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations, hbase_client
from business.interfaces import interfaces
from business.business import cumulative_rate, get_month_account_yield, hbase_result_deal, special_date
__author__ = "chenk"


class GetAccountYield(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "user_daily_data"])
    INTERFACE_NAME = "general/get_account_yield"
    COLUMNS = ["data_list", "count"]

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
        hbase_result_list = []
        hbase_result = {}
        init_date_base = special_date.month_add(init_date=self.data.get("init_date"), interval=self.data.get("interval"))
        for init_date_num in range(32 * int(self.data.get("interval"))):
            init_date = special_date.get_date(init_date=init_date_base, delay=init_date_num)
            row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(init_date)])
            hbase_result_origin = hbase_client.getRow(tableName=GetAccountYield.TABLE_NAME, row=row_key)
            hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                       init_date="init_date",
                                                                       income="income", ratio="yield",
                                                                       position="position", daily_income="daily_income",
                                                                       daily_income_ratio="daily_income_ratio")
            if len(hbase_result_dict) > 1:
                hbase_result_list.append(hbase_result_dict)
            if init_date == self.data.get("init_date"):
                break
        hbase_result_list.pop(0)
        hbase_result_list_dealed = cumulative_rate.cal_cumulative_rate_for_element_is_dict(data=hbase_result_list,
                                                                            ratio_column_name="daily_income_ratio",
                                                                    cumulative_ratio_column_name="accumulative_yield",
                                                                                           decimal=4)
        hbase_result.setdefault("data_list", hbase_result_list_dealed)
        hbase_result.setdefault("count", len(hbase_result_list_dealed))
        hbase_command = """get "{0}", "{1}" """.format(GetAccountYield.TABLE_NAME, row_key)

        checking(self=self, class_name=GetAccountYield, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetAccountYield.COLUMNS,
                 special_column=[], list_name=["daily_income_ratio", "yield"])


class GetHisDeliver(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "his_deliver"])
    INTERFACE_NAME = "general/get_his_deliver"
    COLUMNS = ["data_list", "count", "end_date", "begin_date", "info"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetHisDeliver.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        hbase_result_list = []
        hbase_result = {}
        init_date_base = special_date.month_add(init_date=self.data.get("init_date"), interval=self.data.get("interval"))
        init_date_base = special_date.get_date(init_date=init_date_base, delay=1)
        for i, init_date_num in enumerate(range(32 * int(self.data.get("interval")))):
            init_date = special_date.get_date(init_date=init_date_base, delay=init_date_num)
            if i == 0:
                begin_date = init_date
            row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(init_date),
                                self.data.get("stock_code"), self.data.get("exchange_type")])
            hbase_result_origin = hbase_client.getRow(tableName=GetHisDeliver.TABLE_NAME, row=row_key)
            hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                       func={"deliver_content": eval},
                                                                       deliver_content="deliver_content")
            if len(hbase_result_dict) > 0:
                hbase_result_list.append(hbase_result_dict.get("deliver_content")[0])

            if init_date == self.data.get("init_date"):
                end_date = init_date
                break

        hbase_result.setdefault("data_list", hbase_result_list)
        hbase_result.setdefault("count", len(hbase_result_list))
        hbase_result.setdefault("begin_date", begin_date)
        hbase_result.setdefault("end_date", end_date)
        hbase_command = """get "{0}", "{1}" """.format(GetHisDeliver.TABLE_NAME, row_key)

        checking(self=self, class_name=GetHisDeliver, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetHisDeliver.COLUMNS,)


class CreditGetAccountYield(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "credit_user_daily_data"])
    INTERFACE_NAME = "credit/get_account_yield"
    COLUMNS = ["data_list", "count"]

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
        url = self.url_prefix + CreditGetAccountYield.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        hbase_result_list = []
        hbase_result = {}
        init_date_base = special_date.month_add(init_date=self.data.get("init_date"), interval=self.data.get("interval"))
        for init_date_num in range(32 * int(self.data.get("interval"))):
            init_date = special_date.get_date(init_date=init_date_base, delay=init_date_num)
            row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(init_date)])
            hbase_result_origin = hbase_client.getRow(tableName=CreditGetAccountYield.TABLE_NAME, row=row_key)
            hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                       init_date="init_date",
                                                                       income="income", ratio="yield",
                                                                       total_asset="total_asset", net_asset="net_asset",
                                                                       assure_ratio="assure_ratio",
                                                                       hs_daily_income_ratio="hs_daily_income_ratio")
            if len(hbase_result_dict) > 1:
                hbase_result_list.append(hbase_result_dict)
            if init_date == self.data.get("init_date"):
                break
        hbase_result_list.pop(0)
        hbase_result_list_dealed = cumulative_rate.cal_cumulative_rate_for_element_is_dict(data=hbase_result_list,
                                                                            ratio_column_name="yield",
                                                                    cumulative_ratio_column_name="accumulative_yield")
        hbase_result.setdefault("data_list", hbase_result_list_dealed)
        hbase_result.setdefault("count", len(hbase_result_list_dealed))
        hbase_command = """get "{0}", "{1}" """.format(CreditGetAccountYield.TABLE_NAME, row_key)

        checking(self=self, class_name=CreditGetAccountYield, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetAccountYield.COLUMNS,
                 special_column=[], list_name=["daily_income_ratio", "yield"])


class GetIncomeAnalyze(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "general/get_income_analyze"
    COLUMNS = ['fund_out', 'fund_in', 'begin_asset', 'last_asset', 'net_inflow', 'asset_income', 'asset_yield',
               'stock_income', 'wit_income', 'fund_income', 'other_income', 'draw_back', 'fund_rank', 'bond_income',
               'otc_income', 'other_assets_income', 'begin_date', 'end_date', 'bshare_income']

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
        # home_page_data 获取主要数据字段
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetIncomeAnalyze.TABLE_NAME, row=row_key)

        hbase_result = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                              fund_out='fund_out', fund_in='fund_in',
                                                              begin_asset='begin_asset', last_asset='last_asset',
                                                              net_inflow='net_inflow', asset_income='asset_income',
                                                              asset_yield='asset_yield', stock_income='stock_income',
                                                              wit_income='wit_income', fund_income='fund_income',
                                                              other_income='other_income', draw_back='draw_back',
                                                              fund_rank='fund_rank', bond_income='bond_income',
                                                              otc_income='otc_income',
                                                              other_assets_income='other_assets_income',
                                                              begin_date='begin_date', end_date='end_date',
                                                              bshare_income='bshare_income')
        hbase_command = """get "{0}", "{1}" """.format(GetIncomeAnalyze.TABLE_NAME, row_key)

        # interval_fund_rank 获取 yield 并计算 fund_rank
        row_key = ",".join([self.data.get("interval"), init_date_to_cal_date(self.info.get("init_date"))])
        _hbase_result_origin = hbase_client.getRow(tableName="zhfx_gl_chenk:interval_fund_rank", row=row_key)
        hbase_command = """get "{0}", "{1}" """.format("zhfx_gl_chenk:interval_fund_rank", row_key)
        _hbase_result = hbase_result_deal.hbase_result_to_dict(hbase_result=_hbase_result_origin,
                                                               func={"fund_content": eval},
                                                              fund_content="fund_content")

        success_num = 0
        fail_num = 0
        ratio = hbase_result.get("asset_yield")
        for each in _hbase_result.get("fund_content"):
            if ratio > eval(each.get("yield")):
                success_num += 1
            else:
                fail_num += 1
        fund_rank = round(success_num / (success_num + fail_num), 4)
        hbase_result.setdefault("fund_rank", fund_rank)
        checking(self=self, class_name=GetIncomeAnalyze, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetIncomeAnalyze.COLUMNS,
                 calculation=["net_inflow", "fund_in", "-", "fund_out"],
                 list_name=["bond_income", "wit_income", "otc_income", "fund_income", "other_assets_income",
                            "other_income"], special_column=["asset_yield", "draw_back"])


class CreditGetIncomeAnalyze(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "credit/get_income_analyze"
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
        url = self.url_prefix + CreditGetIncomeAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetIncomeAnalyze.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(CreditGetIncomeAnalyze.TABLE_NAME, row_key)

        checking(self=self, class_name=CreditGetIncomeAnalyze, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetIncomeAnalyze.COLUMNS,
                 calculation=["net_inflow", "fund_in", "-", "fund_out"],
                 list_name=["bond_income", "wit_income", "otc_income", "fund_income", "other_assets_income",
                            "other_income"], special_column=["assure_yield"])


class GetInvestAnalyze(unittest.TestCase):
    # 多个表取数据源
    TABLE_NAME = ["".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "basic_data"]), "".join([get_configurations.get_target_section(section='database_guolian').
                                                  get("database_prefix"), "home_page_data"])]
    INTERFACE_NAME = "general/get_invest_analyze"
    COLUMNS = ["income_ability", "timing_ability", "discipline_ability", "choose_ability",
               "risk_control_ability", "trans_ability", "ability_rank"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_my_ability(self):
        """"""
        url = self.url_prefix + GetInvestAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # 只验证出参结果中的 my_ability
        interface_result = interface_result.get("my_ability")

        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_command = """get "{0}", "{1}" """.format(GetInvestAnalyze.TABLE_NAME[1], row_key)
        hbase_result = hbase_client.getRow(tableName=GetInvestAnalyze.TABLE_NAME[1], row=row_key)

        checking(self=self, class_name=GetInvestAnalyze, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetInvestAnalyze.COLUMNS,
                 )


class CreditGetInvestAnalyze(unittest.TestCase):
    # 多个表取数据源
    TABLE_NAME = ["".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "credit_basic_data"]), "".join([get_configurations.get_target_section(section='database_guolian').
                                                  get("database_prefix"), "home_page_data"])]
    INTERFACE_NAME = "credit/get_invest_analyze"
    COLUMNS = ["income_ability", "timing_ability", "discipline_ability", "choose_ability",
               "risk_control_ability", "trans_ability", "ability_rank"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        interval=cls.info.get("interval"), init_date=cls.info.get("init_date"))

    def test_my_ability(self):
        """"""
        url = self.url_prefix + CreditGetInvestAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # 只验证出参结果中的 my_ability
        interface_result = interface_result.get("my_ability")

        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"),
                            "7", init_date_to_cal_date(self.info.get("init_date"))])
        hbase_command = """get "{0}", "{1}" """.format(CreditGetInvestAnalyze.TABLE_NAME[1], row_key)
        hbase_result = hbase_client.getRow(tableName=CreditGetInvestAnalyze.TABLE_NAME[1], row=row_key)

        checking(self=self, class_name=CreditGetInvestAnalyze, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetInvestAnalyze.COLUMNS,
                 )

    def test_avg_ability(self):
        """"""
        url = self.url_prefix + CreditGetInvestAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # 只验证出参结果中的 avg_ability
        interface_result = interface_result.get("avg_ability")

        row_key = ",".join([self.data.get("interval"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_command = """get "{0}", "{1}" """.format(CreditGetInvestAnalyze.TABLE_NAME[0], row_key)
        hbase_result = hbase_client.getRow(tableName=CreditGetInvestAnalyze.TABLE_NAME[0], row=row_key)

        checking(self=self, class_name=CreditGetInvestAnalyze, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetInvestAnalyze.COLUMNS,
                 special_column=["ability_rank"]
                 )


class GetReplayAnalyze(unittest.TestCase):
    # 多个表取数据源
    TABLE_NAME = ["".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "basic_data"]), "".join([get_configurations.get_target_section(section='database_guolian').
                                                  get("database_prefix"), "home_page_data"])]
    INTERFACE_NAME = "general/get_replay_analyze"
    COLUMNS = ["income_ability", "timing_ability", "discipline_ability", "choose_ability",
               "risk_control_ability", "trans_ability", "ability_rank"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_my_ability(self):
        """"""
        url = self.url_prefix + GetReplayAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # 只验证出参结果中的 my_ability
        interface_result = interface_result.get("my_ability")

        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_command = """get "{0}", "{1}" """.format(GetReplayAnalyze.TABLE_NAME[1], row_key)
        hbase_result = hbase_client.getRow(tableName=GetReplayAnalyze.TABLE_NAME[1], row=row_key)

        checking(self=self, class_name=GetReplayAnalyze, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetReplayAnalyze.COLUMNS,
                 )

    def test_avg_ability(self):
        """"""
        url = self.url_prefix + GetInvestAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        # 只验证出参结果中的 avg_ability
        interface_result = interface_result.get("avg_ability")

        row_key = ",".join([self.data.get("interval"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_command = """get "{0}", "{1}" """.format(GetInvestAnalyze.TABLE_NAME[0], row_key)
        hbase_result = hbase_client.getRow(tableName=GetInvestAnalyze.TABLE_NAME[0], row=row_key)

        checking(self=self, class_name=GetInvestAnalyze, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetInvestAnalyze.COLUMNS,
                 special_column=["ability_rank"]
                 )


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
                 special_column=["is_last", "asset"])


class CreditGetLastAsset(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "credit_user_daily_data"])
    INTERFACE_NAME = "credit/get_last_asset"
    COLUMNS = ["asset", ]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        trade_type="1", asset_prop="7")

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetLastAsset.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"),
                            init_date_to_cal_date(self.info.get("init_date"))])

        hbase_result_origin = hbase_client.getRow(tableName=CreditGetLastAsset.TABLE_NAME, row=row_key)
        # self.assertTrue(0, msg="{0}--".format(hbase_result))
        hbase_command = """get "{0}", "{1}" """.format(CreditGetLastAsset.TABLE_NAME, row_key)
        checking(self=self, class_name=CreditGetLastAsset, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetLastAsset.COLUMNS,
                 list_name=["net_asset", "asset"])


class GetPosition(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "user_daily_data"])
    INTERFACE_NAME = "general/get_position"
    COLUMNS = ["data_list", "count"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetPosition.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)

        # cashCurveDataList
        hbase_result = {}
        hbase_result_deal_list = []
        init_date_base = special_date.month_add(init_date=self.info.get("init_date"),
                                                interval=self.info.get("interval"))
        init_date_first = special_date.get_date(init_date=init_date_base, delay=1)
        # 保证循环次数大于等于需要获取的数据次数
        for i in range(31 * int(self.info.get("interval"))):
            init_date = special_date.get_date(init_date_first, i)
            row_key = ",".join([self.data.get("fund_account_reversed"),
                                init_date_to_cal_date(init_date)])

            # hbase 获取的原始数据
            hbase_result_origin = hbase_client.getRow(tableName=GetPosition.TABLE_NAME, row=row_key)
            # 提取hbase结果的数据 转化成 字典数据
            hbase_result_deal_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                            func={"position": eval},
                                                                            init_date=int(init_date),
                                                                            position="position")
            # 若转化后的字典数据存在数据， 则放在list 里面
            if len(hbase_result_deal_dict) > 0:
                hbase_result_deal_list.append(hbase_result_deal_dict)

            # 上述循环次数可能大于实际要获取的次数， 故作此判断
            if init_date == self.info.get("init_date"):
                break

        hbase_result.setdefault("data_list", hbase_result_deal_list)
        hbase_result.setdefault("count", len(hbase_result_deal_list))
        # self.assertTrue(0, msg="{0}--".format(hbase_result))
        hbase_command = """get "{0}", "{1}" """.format(GetPosition.TABLE_NAME, row_key)
        checking(self=self, class_name=GetPosition, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetPosition.COLUMNS)


class GetStockPreference(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "general/get_stock_preference"
    COLUMNS = ["stock_code", "stock_name", "exchange_type", "stock_type", "income", "hold_day"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetStockPreference.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.info.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetStockPreference.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetStockPreference.TABLE_NAME, row_key)

        checking(self=self, class_name=GetStockPreference, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetStockPreference.COLUMNS)


class CreditGetStockPreference(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "general/get_stock_preference"
    COLUMNS = ["stock_code", "stock_name", "exchange_type", "stock_type", "income", "hold_day"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetStockPreference.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.info.get("interval"), "1",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetStockPreference.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(CreditGetStockPreference.TABLE_NAME, row_key)

        checking(self=self, class_name=CreditGetStockPreference, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetStockPreference.COLUMNS)


class CreditGetStockPreference(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "credit/get_stock_preference"
    COLUMNS = ["stock_code", "stock_name", "exchange_type", "stock_type", "income", "hold_day"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetStockPreference.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.info.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetStockPreference.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetStockPreference.TABLE_NAME, row_key)
        checking(self=self, class_name=CreditGetStockPreference, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetStockPreference.COLUMNS)


class GetTop3Data(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "general/get_top3_data"
    COLUMNS = ["stock_code", "stock_name", "exchange_type", "stock_type", "income", "hold_day", "status",
               "income_rate", "amount", "hold_status", "money_type", "trade_type"]
    COLUMNS = ["profitList", "lossList"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetTop3Data.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetTop3Data.TABLE_NAME, row=row_key)
        hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                   first_profit="first_profit",
                                                                   second_profit="second_profit",
                                                                   third_profit="third_profit",
                                                                   first_loss="first_loss",
                                                                   second_loss="second_loss",
                                                                   third_loss="third_loss")
        # 处理hbase数据为接口返回的形式
        hbase_result_dict.setdefault("profitList", [])
        hbase_result_dict.setdefault("lossList", [])
        for i, top3 in enumerate(["first_profit", "second_profit", "third_profit", "first_loss",
                               "second_loss", "third_loss"]):
            if i < 3:
                hbase_result_dict.get("profitList").append(hbase_result_dict.get(top3))
            else:
                hbase_result_dict.get("lossList").append(hbase_result_dict.get(top3))
            hbase_result_dict.pop(top3)

        hbase_command = """get "{0}", "{1}" """.format(GetTop3Data.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="hbase: {0}\n; interface: {1}".format(hbase_result_dict, interface_result))
        checking(self=self, class_name=GetTop3Data, sql_result=hbase_result_dict,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetTop3Data.COLUMNS)


class CreditGetTop3Data(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "credit/get_top3_data"
    COLUMNS = ["stock_code", "stock_name", "exchange_type", "stock_type", "income", "hold_day", "status",
               "income_rate", "amount", "hold_status", "money_type", "trade_type"]
    COLUMNS = ["profitList", "lossList"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        trade_type="1", asset_prop="7")

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetTop3Data.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetTop3Data.TABLE_NAME, row=row_key)
        hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                   first_profit="first_profit",
                                                                   second_profit="second_profit",
                                                                   third_profit="third_profit",
                                                                   first_loss="first_loss",
                                                                   second_loss="second_loss",
                                                                   third_loss="third_loss")
        # 处理hbase数据为接口返回的形式
        hbase_result_dict.setdefault("profitList", [])
        hbase_result_dict.setdefault("lossList", [])
        for i, top3 in enumerate(["first_profit", "second_profit", "third_profit", "first_loss",
                               "second_loss", "third_loss"]):
            if i < 3:
                hbase_result_dict.get("profitList").append(hbase_result_dict.get(top3))
            else:
                hbase_result_dict.get("lossList").append(hbase_result_dict.get(top3))
            hbase_result_dict.pop(top3)

        hbase_command = """get "{0}", "{1}" """.format(CreditGetTop3Data.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="hbase: {0}\n; interface: {1}".format(hbase_result_dict, interface_result))
        checking(self=self, class_name=CreditGetTop3Data, sql_result=hbase_result_dict,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetTop3Data.COLUMNS)


class GetTradeAnalyze(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "general/get_trade_analyze"
    COLUMNS = ["tradeAnalyzeData"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetTradeAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            self.info.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetTradeAnalyze.TABLE_NAME, row=row_key)
        hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result_origin, func={"stock_count": int,
                                                                                          "profit_count": int,
                                                                                          "win_ratio": eval,
                                                                                          "win_ratio_rank": eval,
                                                                                          "slo_stock_count": int,
                                                                                          "slo_profit_count": int,
                                                                                          "slo_win_ratio": eval,
                                                                                          "slo_win_ratio_rank": eval,
                                                                                          "stock_exhcange_right": int,
                                                                                              },
                                                                   stock_count="stock_count",
                                                                   profit_count="profit_count",
                                                                   win_ratio="win_ratio",
                                                                   rank="0",
                                                                   win_ratio_rank="win_ratio_rank",
                                                                   slo_stock_count="slo_stock_count",
                                                                   slo_profit_count="slo_profit_count",
                                                                   slo_win_ratio="slo_win_ratio",
                                                                   slo_win_ratio_rank="slo_win_ratio_rank",
                                                                   stock_exhcange_right="stock_exhcange_right")
        hbase_result_dict.update({"rank": hbase_result_dict.get("win_ratio_rank")})
        tmp_dict = hbase_result_dict.copy()
        hbase_result_dict.setdefault("tradeAnalyzeData", tmp_dict)
        hbase_command = """get "{0}", "{1}" """.format(GetTradeAnalyze.TABLE_NAME, row_key)
        checking(self=self, class_name=GetTradeAnalyze, sql_result=hbase_result_dict,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, list_name=["financial_hold_data", "financial_hold_data_list"],
                 table_columns=GetTradeAnalyze.COLUMNS, )


class CreditGetTradeAnalyze(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "home_page_data"])
    INTERFACE_NAME = "credit/get_trade_analyze"
    COLUMNS = ["financing_data", "loan_data"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        asset_prop="7")

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetTradeAnalyze.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"),
                            self.data.get("asset_prop"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetTradeAnalyze.TABLE_NAME, row=row_key)
        hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result_origin, func={"stock_count": int,
                                                                                          "profit_count": int,
                                                                                          "win_ratio": eval,
                                                                                          "win_ratio_rank": eval,
                                                                                          "slo_stock_count": int,
                                                                                          "slo_profit_count": int,
                                                                                          "slo_win_ratio": eval,
                                                                                          "slo_win_ratio_rank": eval,
                                                                                          "stock_exhcange_right": int,
                                                                                              },
                                                                   stock_count="stock_count",
                                                                   profit_count="profit_count",
                                                                   win_ratio="win_ratio",
                                                                   rank="0",
                                                                   win_ratio_rank="win_ratio_rank",
                                                                   slo_stock_count="slo_stock_count",
                                                                   slo_profit_count="slo_profit_count",
                                                                   slo_win_ratio="slo_win_ratio",
                                                                   slo_win_ratio_rank="slo_win_ratio_rank",
                                                                   stock_exhcange_right="stock_exhcange_right")
        hbase_result_dict_dealed = {}
        # 股票
        tmp_dict = {}
        tmp_dict.update({"stock_count": hbase_result_dict.get("stock_count")})
        tmp_dict.update({"profit_count": hbase_result_dict.get("profit_count")})
        tmp_dict.update({"win_ratio": hbase_result_dict.get("win_ratio")})
        tmp_dict.update({"rank": hbase_result_dict.get("win_ratio_rank")})
        tmp_dict.update({"win_ratio_rank": hbase_result_dict.get("win_ratio_rank")})
        tmp_dict.update({"slo_stock_count": hbase_result_dict.get("stock_count")})
        tmp_dict.update({"slo_profit_count": hbase_result_dict.get("profit_count")})
        tmp_dict.update({"slo_win_ratio": hbase_result_dict.get("win_ratio")})
        tmp_dict.update({"slo_win_ratio_rank": hbase_result_dict.get("win_ratio_rank")})
        tmp_dict.update({"stock_exhcange_right": hbase_result_dict.get("stock_exhcange_right")})
        hbase_result_dict_dealed.setdefault("financing_data", tmp_dict)
        # 融券
        tmp_dict = {}
        tmp_dict.update({"stock_count": hbase_result_dict.get("slo_stock_count")})
        tmp_dict.update({"profit_count": hbase_result_dict.get("slo_profit_count")})
        tmp_dict.update({"win_ratio": hbase_result_dict.get("slo_win_ratio")})
        tmp_dict.update({"rank": hbase_result_dict.get("slo_win_ratio_rank")})
        tmp_dict.update({"win_ratio_rank": hbase_result_dict.get("slo_win_ratio_rank")})
        tmp_dict.update({"slo_stock_count": hbase_result_dict.get("slo_stock_count")})
        tmp_dict.update({"slo_profit_count": hbase_result_dict.get("slo_profit_count")})
        tmp_dict.update({"slo_win_ratio": hbase_result_dict.get("slo_win_ratio")})
        tmp_dict.update({"slo_win_ratio_rank": hbase_result_dict.get("slo_win_ratio_rank")})
        tmp_dict.update({"stock_exhcange_right": hbase_result_dict.get("stock_exhcange_right")})
        hbase_result_dict_dealed.setdefault("loan_data", tmp_dict)
        hbase_command = """get "{0}", "{1}" """.format(CreditGetTradeAnalyze.TABLE_NAME, row_key)
        checking(self=self, class_name=CreditGetTradeAnalyze, sql_result=hbase_result_dict_dealed,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetTradeAnalyze.COLUMNS, )


class GetTradeDistribution(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_distribution"])
    INTERFACE_NAME = "general/get_trade_distribution"
    COLUMNS = ["data_list", "count"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetTradeDistribution.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"),
                            "0", init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetTradeDistribution.TABLE_NAME, row=row_key)
        hbase_result = hbase_result_deal.deal(hbase_result=hbase_result_origin, is_json_content=True,
                               list_name=["distribute_content", "data_list"])
        hbase_result.setdefault("count", len(hbase_result.get("data_list", 0))+7)
        hbase_command = """get "{0}", "{1}" """.format(GetTradeDistribution.TABLE_NAME, row_key)
        checking(self=self, class_name=GetTradeDistribution, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetTradeDistribution.COLUMNS,
                 list_name=["distribute_content", "data_list"])


class CreditGetTradeDistribution(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_distribution"])
    INTERFACE_NAME = "credit/get_trade_distribution"
    COLUMNS = ["data_list", "count"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        trade_type="1")

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetTradeDistribution.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"),
                            self.data.get("trade_type"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetTradeDistribution.TABLE_NAME, row=row_key)
        hbase_result = hbase_result_deal.deal(hbase_result=hbase_result_origin, is_json_content=True,
                               list_name=["distribute_content", "data_list"])
        hbase_result.setdefault("count", len(hbase_result.get("data_list", 0))+7)
        hbase_command = """get "{0}", "{1}" """.format(CreditGetTradeDistribution.TABLE_NAME, row_key)
        checking(self=self, class_name=CreditGetTradeDistribution, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetTradeDistribution.COLUMNS,
                 list_name=["distribute_content", "data_list"])


class GetTradeStyle(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "general/get_trade_style"
    COLUMNS = ['avg_hold_day', 'avg_hold_day_rank', 'draw_back', 'draw_back_rank', 'win_ratio', 'win_ratio_rank',
               'avg_market_value', 'avg_market_value_rank']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetTradeStyle.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetTradeStyle.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(GetTradeStyle.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="command {0}".format(hbase_command))
        checking(self=self, class_name=GetTradeStyle, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetTradeStyle.COLUMNS,)


class CreditGetTradeStyle(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "general/get_trade_style"
    COLUMNS = ['avg_hold_day', 'avg_hold_day_rank', 'draw_back', 'draw_back_rank', 'avg_position',
               'avg_position_rank', 'win_ratio', 'win_ratio_rank', 'fund_utilize', 'fund_utilize_rank',
               'avg_market_value', 'avg_market_value_rank']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        trade_type="1")

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetTradeStyle.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"),
                            self.data.get("trade_type"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetTradeStyle.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(CreditGetTradeStyle.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="command {0}".format(hbase_command))
        checking(self=self, class_name=CreditGetTradeStyle, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetTradeStyle.COLUMNS,
                 special_column=["fund_utilize", "fund_utilize_rank", "avg_position", "avg_position_rank"])


class CreditGetTradeStyle(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "credit/get_trade_style"
    COLUMNS = ['avg_hold_day', 'avg_hold_day_rank', 'draw_back', 'draw_back_rank', 'avg_assure_ratio',
               'avg_assure_ratio_rank', 'win_ratio', 'win_ratio_rank', 'fund_utilize', 'fund_utilize_rank']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        trade_type="1")

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetTradeStyle.INTERFACE_NAME
        # self.data.update({"trade_type": "1"})
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"), "1",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetTradeStyle.TABLE_NAME, row=row_key)
        hbase_command = """get "{0}", "{1}" """.format(CreditGetTradeStyle.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="command {0}".format(hbase_command))
        checking(self=self, class_name=CreditGetTradeStyle, sql_result=hbase_result_origin,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetTradeStyle.COLUMNS,
                 special_column=["fund_utilize", "fund_utilize_rank"])


class GetTradeStatistics(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "general/get_trade_statistics"
    COLUMNS = ['trade_balance', 'buy_count', 'sell_count', 'trade_stock_count', 'trade_frequency',
               'buy_amount', 'sell_amount', 'stock_count']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetTradeStatistics.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.info.get("interval"),
                            "0", init_date_to_cal_date(self.info.get("init_date"))])

        hbase_result_origin = hbase_client.getRow(tableName=GetTradeStatistics.TABLE_NAME, row=row_key)
        hbase_result = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                              trade_balance="trade_balance",
                                                              buy_amount="buy_amount",
                                                              trade_stock_count="trade_stock_count",
                                                              trade_frequency="trade_frequency",
                                                              sell_amount="sell_amount",
                                                              stock_count="stock_count")
        hbase_result.setdefault("buy_count", hbase_result.get("buy_amount"))
        hbase_result.setdefault("sell_count", hbase_result.get("sell_amount"))
        hbase_command = """get "{0}", "{1}" """.format(GetTradeStatistics.TABLE_NAME, row_key)

        checking(self=self, class_name=GetTradeStatistics, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetTradeStatistics.COLUMNS,
                 special_column=['trade_stock_count'])


class CreditGetTradeStatistics(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "trade_statistics"])
    INTERFACE_NAME = "credit/get_trade_statistics"
    COLUMNS = ['trade_balance', 'buy_count', 'sell_count', 'trade_stock_count', 'trade_frequency',
               'buy_amount', 'sell_amount', 'stock_count']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        init_date=cls.info.get("init_date"), interval=cls.info.get("interval"),
                                        trade_type="1")

    def test_normal(self):
        """"""
        url = self.url_prefix + GetTradeStatistics.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.info.get("interval"),
                            self.info.get("trade_type"), init_date_to_cal_date(self.info.get("init_date"))])

        hbase_result_origin = hbase_client.getRow(tableName=GetTradeStatistics.TABLE_NAME, row=row_key)
        hbase_result = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                              trade_balance="trade_balance",
                                                              buy_amount="buy_amount",
                                                              trade_stock_count="trade_stock_count",
                                                              trade_frequency="trade_frequency",
                                                              sell_amount="sell_amount",
                                                              stock_count="stock_count")
        hbase_result.setdefault("buy_count", hbase_result.get("buy_amount"))
        hbase_result.setdefault("sell_count", hbase_result.get("sell_amount"))
        hbase_command = """get "{0}", "{1}" """.format(GetTradeStatistics.TABLE_NAME, row_key)

        checking(self=self, class_name=GetTradeStatistics, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=False,
                 sql=hbase_command, url=url, data=data, table_columns=GetTradeStatistics.COLUMNS,
                 list_name=["buy_amount", "buy_count", "sell_amount", "sell_count"],
                 special_column=['trade_stock_count'])


class GetAssureRatio(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "credit_user_daily_data"])
    INTERFACE_NAME = "credit/get_assure_ratio"
    COLUMNS = ['data_list']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetAssureRatio.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        init_date_base = special_date.month_add(init_date=self.data.get("init_date"), interval=self.data.get('interval'))
        init_date_first = special_date.get_date(init_date=init_date_base, delay=1)
        hbase_result_list = []
        hbase_result_dict = {}
        keyword.kwlist.remove("yield")
        for i in range(31 * int(self.data.get('interval'))):
            init_date = special_date.get_date(init_date=init_date_first, delay=i)
            row_key = ",".join([self.data.get("fund_account_reversed"), init_date_to_cal_date(init_date)])
            hbase_result_origin = hbase_client.getRow(tableName=GetAssureRatio.TABLE_NAME, row=row_key)
            hbase_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin, init_date="init_date",
                                                                income="income", assure_ratio="assure_ratio",
                                                                ratio="yield", accumulative_yield="accumulative_yield")
            if len(hbase_dict) >= 1:
                hbase_result_list.append(hbase_dict)
            if init_date == self.data.get("init_date"):
                break
        hbase_result_dict.setdefault("data_list", hbase_result_list)
        hbase_result_dict.setdefault("count", len(hbase_result_list))
        hbase_command = """get "{0}", "{1}" """.format(GetAssureRatio.TABLE_NAME, row_key)

        checking(self=self, class_name=GetAssureRatio, sql_result=hbase_result_dict,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetAssureRatio.COLUMNS,)


class GetStockList(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "interval_stock"])
    INTERFACE_NAME = "general/get_stock_list"
    COLUMNS = ['data_list']

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetAssureRatio.INTERFACE_NAME
        self.data.setdefault("income_order", "1")
        self.data.setdefault("status_order", "1")
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        init_date_base = special_date.month_add(init_date=self.data.get("init_date"), interval=self.data.get('interval'))
        init_date_first = special_date.get_date(init_date=init_date_base, delay=1)
        hbase_result_list = []
        hbase_result_dict = {}
        for i in range(31 * int(self.data.get('interval'))):
            init_date = special_date.get_date(init_date=init_date_first, delay=i)
            row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval_type"),
                                "0", init_date_to_cal_date(self.data.get("init_date"))])
            hbase_result_origin = hbase_client.getRow(tableName=GetAssureRatio.TABLE_NAME, row=row_key)
            hbase_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin, init_date="init_date",
                                                                income="income", assure_ratio="assure_ratio",
                                                                ratio="yield", accumulative_yield="accumulative_yield")
            if len(hbase_dict) >= 1:
                hbase_result_list.append(hbase_dict)
            if init_date == self.data.get("init_date"):
                break
        hbase_result_dict.setdefault("data_list", hbase_result_list)
        hbase_result_dict.setdefault("count", len(hbase_result_list))
        hbase_command = """get "{0}", "{1}" """.format(GetAssureRatio.TABLE_NAME, row_key)

        checking(self=self, class_name=GetAssureRatio, sql_result=hbase_result_dict,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetAssureRatio.COLUMNS,)


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


class GetStockList(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "interval_stock"])
    INTERFACE_NAME = "general/get_stock_list"
    COLUMNS = ["data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(option_info=cls.info)
        cls.data.setdefault("income_order", 1)
        cls.data.setdefault("status_order", 1)

    def test_normal(self):
        """"""
        url = self.url_prefix + GetStockList.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("fund_account_reversed"), self.data.get("interval"), "0",
                            init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=GetStockList.TABLE_NAME, row=row_key)
        hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                   stock_content="stock_content",)
        # 处理hbase数据为接口返回的形式
        hbase_result = {}
        if len(hbase_result_dict) > 0:
            hbase_result.setdefault("data_list", eval(hbase_result_dict.get("stock_content")))

        hbase_command = """get "{0}", "{1}" """.format(GetStockList.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="hbase: {0}\n; interface: {1}".format(hbase_result_dict, interface_result))
        checking(self=self, class_name=GetStockList, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=GetStockList.COLUMNS,
                 deal_column=["hold_day", int])


class CreditGetStockList(unittest.TestCase):
    TABLE_NAME = "".join([get_configurations.get_target_section(section='database_guolian').get("database_prefix"),
                          "interval_stock"])
    INTERFACE_NAME = "credit/get_stock_list"
    COLUMNS = ["data_list"]

    @classmethod
    def setUpClass(cls):
        urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.info = get_configurations.get_target_section(section='guolian_info')
        print("here is info:\n", cls.info)
        cls.url_prefix = urls_prefix.get("analysis_guolian_prefix")
        cls.data = get_basic_paramaters(fund_account=cls.info.get("credit_fund_account"),
                                        credit_fund_account=cls.info.get("credit_fund_account"),
                                        interval=cls.info.get("interval"), init_date=cls.info.get("init_date"),
                                        page_no=cls.info.get("page_no"), page_size=cls.info.get("page_size"),
                                        trade_type="1")
        cls.data.setdefault("income_order", 1)
        cls.data.setdefault("status_order", 1)

    def test_normal(self):
        """"""
        url = self.url_prefix + CreditGetStockList.INTERFACE_NAME
        data = str(self.data.copy()).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        row_key = ",".join([self.data.get("credit_fund_account_reversed"), self.data.get("interval"),
                            self.data.get("trade_type"), init_date_to_cal_date(self.info.get("init_date"))])
        hbase_result_origin = hbase_client.getRow(tableName=CreditGetStockList.TABLE_NAME, row=row_key)
        hbase_result_dict = hbase_result_deal.hbase_result_to_dict(hbase_result=hbase_result_origin,
                                                                   stock_content="stock_content",)
        # 处理hbase数据为接口返回的形式
        hbase_result = {}
        if len(hbase_result_dict) > 0:
            hbase_result.setdefault("data_list", eval(hbase_result_dict.get("stock_content")))

        hbase_command = """get "{0}", "{1}" """.format(CreditGetStockList.TABLE_NAME, row_key)
        # self.assertTrue(0, msg="hbase: {0}\n; interface: {1}".format(hbase_result_dict, interface_result))
        checking(self=self, class_name=CreditGetStockList, sql_result=hbase_result,
                 interface_result=interface_result, is_hbase_result=True, is_json_content=True,
                 sql=hbase_command, url=url, data=data, table_columns=CreditGetStockList.COLUMNS,
                 deal_column=["hold_day", int])


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
    calculation is a list like {"column": ["new_column", "column1", "+", "column2"]}. 
    The first one is the new column which will returned in interface. 
    The others can be joined together as a arithmetic expressions. 
    :return: 
    """
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
                    # origin_column = column
                    new_column = column
                    # list_name 允许存在多组数据， eg: [old_1, new_1, old_2, new_2]
                    if params.get("list_name").index(column) / 2 == 0:
                        tmp = params.get("list_name").index(column) + 1
                    else:
                        tmp = params.get("list_name").index(column) - 1
                    # new_column = params.get("list_name")[tmp]
                    origin_column = params.get("list_name")[tmp]
                    sql_result.setdefault(new_column, sql_result.get(origin_column))

        try:
            for column in params.get("table_columns"):
                # 统一日志信息
                msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
                Hbase result is\n{4}\n column is [{5}]".format(params.get("sql"), params.get("url"), params.get("data"),
                                                               interface_result, sql_result, column)
                # 检验字段在 是一个list 类型 list 中的每个元素是一个dict， 那么对比list 里面每一个 dict 中的元素的数据
                if isinstance(sql_result.get(column), list):
                    for i, info in enumerate(sql_result.get(column)):
                        if not isinstance(info, dict):
                            info = eval(info)
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
                # 若不是过滤字段，需验证； 否则，不验证
                elif column not in params.get("special_column", []):
                    # 若字段已被重新命名
                    # if column in params.get("list_name"):
                    #     self.assertEqual(str(sql_result.get(column)), str(interface_result.get(column)), msg=msg_model)
                    self.assertEqual(str(sql_result.get(column, '')), str(interface_result.get(column)), msg=msg_model)
                    continue

        except TypeError:
            if not locals().get(column):
                column = "cant get."
            self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                            Current is checking column {3}".format(params.get("sql"), params.get("url"),
                                                                   params.get("data"), column))

    else:
        # 统一日志信息
        msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
        Hbase result is\n{4}\n ".format(params.get("sql"), params.get("url"), params.get("data"),
                                                       interface_result, sql_result)
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
    if "credit_fund_account" in params_dic.keys():
        params_dic.setdefault("credit_fund_account_reversed", "".join(reversed(params_dic.get("credit_fund_account"))))
    if stock_trade_times:
        params_dic.setdefault("stock_trade_times",
                              params_dic.get(stock_trade_times[0] + params_dic.get(stock_trade_times[1])))
    return params_dic

if __name__ == "__main__":
    # create unittest tests
    tests = unittest.TestSuite()
    tests.addTest(unittest.makeSuite(testCaseClass=GetAccountYield, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetHisDeliver, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetIncomeAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetIncomeAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetInvestAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetInvestAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetReplayAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetLastAsset, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetLastAsset, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetPosition, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetTradeDistribution, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetTradeDistribution, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetStockPreference, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetStockPreference, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetTop3Data, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetTop3Data, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetTradeAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetTradeAnalyze, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetTradeStyle, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetTradeStyle, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetTradeStatistics, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetTradeStatistics, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetAssureRatio, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=GetStockList, prefix='test'))
    tests.addTest(unittest.makeSuite(testCaseClass=CreditGetStockList, prefix='test'))

    # write unittest result to a file
    with open(r"../export/result_guolian.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

