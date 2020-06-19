# -*- coding: utf-8 -*-
import unittest
import logging
import time
import decimal
import json

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations
from business.interfaces import interfaces
__author__ = "chenk"

class Order:
    TABLE_NAME = "interval_trade_style"
    INTERFACE_NAME = "market/getPersonInfos"
    COLUMNS = ["avg_hold_day", "avg_hold_day_rank", "drawdown", "drawdown_rank", "success_rate",
               "success_rate_rank", "avg_position", "avg_position_rank"]
    @classmethod
    def setUpClass(cls):
        # database_info = get_configurations.get_target_section(section='database')
        # cls.fc_info = get_configurations.get_target_section(section='fc_info')
        cls.urls_prefix = get_configurations.get_target_section(section='url_prefix')
        print("urls_prefix: ", cls.urls_prefix)
        cls.url_prefix = cls.urls_prefix.get("SERVICE_MALL_PREFIX".lower())
        print("here is url_prefix:\n", cls.url_prefix)
        # cls.conn = get_configurations.connect_to_mysql(database_info)
        # cls.cur = cls.conn.cursor()

        # cls.url_prefix = interfaces.get_url_prefix(url_name="SERVICE_MALL_PREFIX")
        data={"account":"yintz19862", "password":"123456"}
        cls.login_info = interfaces.request(url=cls.url_prefix+"market/token", data=data, is_get_method=False)
        # cls.login_info = interfaces.request(url="http://httpbin.org/post", data=json.dumps(data), is_get_method=False)

        print(cls.__class__.__name__, time.time())
        cls.data = {}
        # if fund_account_type == "0":
        #     self.data.setdefault("fundaccount", self.login_info.get("fundaccount")[0])
        # elif fund_account_type == "7":
        #     self.data.setdefault("fundaccountCredit", self.login_info.get("fundaccountCredit")[0])
        # elif fund_account_type.upper() == "B":
        #     self.data.setdefault("fundaccountOption", self.login_info.get("fundaccountOption")[0])
        cls.data.setdefault("access_token", cls.login_info.get("data").get("authInfoDTO").get("accessToken"))



    def test_normal(self):
        
        url = self.url_prefix + Order.INTERFACE_NAME
        self.data.setdefault("searchVal", "1")
        data = str(self.data).replace("'", '"')
        interface_result = interfaces.request(url=url, data=data, is_get_method=False)
        name = interface_result.get("data").get("personInfoList").get("name")
        self.assertIn(name, ['张三', '李四'])
        # sql_tmp = "select "
        # for column in GetTradeStyle.COLUMNS:
            # sql_tmp += column + ", "
        # sql = sql_tmp[:-2] + " from {0} where fund_account = {1} and interval_type = {2};"\
            # .format(GetTradeStyle.TABLE_NAME, self.fc_info["fund_account"], self.fc_info["interval_type"])
        # print("Will execute sql: \n", sql)
        # self.cur.execute(sql)
        # sql_result = self.cur.fetchone()
        # print("sql_result:\n", sql_result)
        # for i, column in enumerate(GetTradeStyle.COLUMNS):
            # if isinstance(sql_result[i], decimal.Decimal):
                # self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)))
                # print(sql_result[i], interface_result.get(column))
            # else:
                # self.assertEqual(sql_result[i], interface_result.get(column))

    @classmethod
    def tearDownClass(cls):
        cls.cur.close()
        cls.conn.close()

if __name__ == "__main__":
    # order = Order()
    # order.setUpClass()
    a = ['fund_out', 'fund_in', 'begin_asset', 'last_asset', 'net_inflow', 'asset_income', 'asset_yield',
         'stock_income', 'wit_income', 'fund_income', 'other_income', 'draw_back', 'fund_rank', 'bond_income',
         'otc_income', 'other_assets_income', 'begin_date', 'end_date', 'bshare_income', 'net_inflow']
    b = ""
    for each in a:
        b += "{0}='{0}', ".format(each)
    print(b)
