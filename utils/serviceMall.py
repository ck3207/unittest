# -*- coding: utf-8 -*-
import unittest
import logging
import time
import decimal

from HTMLTestRunner import HTMLTestRunner
from get_configurations import get_configurations
from business.interfaces import interfaces
__author__ = "chenk"

class Order(unittest.TestCase):
    TABLE_NAME = "interval_trade_style"
    INTERFACE_NAME = "market/getPersonInfos"
    COLUMNS = ["avg_hold_day", "avg_hold_day_rank", "drawdown", "drawdown_rank", "success_rate",
               "success_rate_rank", "avg_position", "avg_position_rank"]
    @classmethod
    def setUpClass(cls):
        # database_info = get_configurations.get_target_section(section='database')
        # cls.fc_info = get_configurations.get_target_section(section='fc_info')
        cls.urls_prefix = get_configurations.get_target_section(section='url_prefix')
        cls.url_prefix = cls.urls_prefix.get("SERVICE_MALL_PREFIX".lower())
        print("here is url_prefix:\n", cls.url_prefix)
        # cls.conn = get_configurations.connect_to_mysql(database_info)
        # cls.cur = cls.conn.cursor()

        # cls.url_prefix = interfaces.get_url_prefix(url_name="SERVICE_MALL_PREFIX")
        cls.login_info = interfaces.request(url=cls.url_prefix+"market/token", 
        data={"account":"yintz19862", "password": "123456"}, is_get_method=False)

        print(cls.__class__.__name__, time.time())
        cls.data = {}
        cls.params = {}
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
        data = self.data
        try:
            interface_result = interfaces.request(url=url, data=data, is_get_method=False)
            name = interface_result.get("data").get("personInfoList")[0].get("name")
            self.assertIn(name, ['张三', '李四'], msg="{0} not in {1}".format(name, ['张三', '李四']))
            self.params.setdefault('name', name)
        except:
            self.assertTrue(0, msg="URL: {0},\n PARAMS:{1}\n RESPONSE:{2}".format(url, self.data, interface_result))
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
                
    def test_normal_1(self):
        url = self.url_prefix + "market/getGoodsList"
        data = self.data.copy()
        data.setdefault('goodsName', "1")
        try:
            interface_result = interfaces.request(url=url, data=data, is_get_method=False)
            goodsId = interface_result.get("data").get("goods_list")[0].get("goodsId")
            self.assertTrue(goodsId, msg="goodsid {0} returned null via interface {1}".format(goodsId, url))
            self.params.setdefault('goodsId', goodsId)
        except:
            self.assertTrue(0, msg="URL: {0},\n PARAMS:{1}\n RESPONSE:{2}".format(url, data, interface_result))

    def test_normal_2(self):
        url = self.url_prefix + "market/getGoodsById"
        data = self.data.copy()
        try:
            # self.assertTrue(0, msg="{}".format(data))
            data.setdefault('id', self.params.get("goodsId"))
        except:
            self.assertTrue(0, msg="{}".format(data))
        try:
            interface_result = interfaces.request(url=url, data=data, is_get_method=False)
            goodsName = interface_result.get("data").get("goodsInfo").get("goodsName")
            self.assertTrue(goodsName, msg="goodsName {0} returned null via interface {1}".format(goodsName, url))
            self.params.setdefault('goodsName', goodsName)
        except:
            self.assertTrue(0, msg="URL: {0},\n PARAMS:{1}\n RESPONSE:{2}".format(url, data, interface_result))

    def test_normal_3(self):
        url = self.url_prefix + "market/getGoodsById"
        data = self.data.copy()
        try:
            # self.assertTrue(0, msg="{}".format(data))
            data.setdefault('id', self.params.get("goodsId"))
        except:
            self.assertTrue(0, msg="{}".format(data))
        try:
            interface_result = interfaces.request(url=url, data=data, is_get_method=False)
            goodsName = interface_result.get("data").get("goodsInfo").get("goodsName")
            self.assertTrue(goodsName, msg="goodsName {0} returned null via interface {1}".format(goodsName, url))
            self.params.setdefault('goodsName', goodsName)
        except:
            self.assertTrue(0, msg="URL: {0},\n PARAMS:{1}\n RESPONSE:{2}".format(url, data, interface_result))
    
    @classmethod
    def tearDownClass(cls):
        pass
        # cls.cur.close()
        # cls.conn.close()

if __name__ == "__main__":
    tests = unittest.TestSuite(unittest.makeSuite(testCaseClass=Order, prefix='test'))
    # tests = unittest.TestSuite()
    # tests.addTest(GetTradeStyle('test_normal'))
    with open(r"result.html", "wb") as f:
        HTMLTestRunner(stream=f, title='测试报告', description='单元测试报告：', verbosity=2).run(tests)

