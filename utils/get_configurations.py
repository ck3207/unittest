# -*- coding: utf-8 -*-
import configparser
import pymysql
import decimal
import cx_Oracle
import time

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase

__author__ = "chenk"

class GetConfigurations:
    def __init__(self, filepath="../conf/conf.ini"):
        self.conf = configparser.ConfigParser()
        self.conf.read(filenames=filepath, encoding="utf-8")

    def get_target_section(self, section):
        info = {}
        for option in self.conf.options(section=section):
            info.setdefault(option, self.conf.get(section=section, option=option))
        return info

    def connect_to_mysql(self, database_info):
        try:
            conn = pymysql.connect(host=database_info["host"], port=int(database_info["port"]), user=database_info["user"],
                                   password=database_info["password"], charset=database_info["charset"],
                                   database=database_info["database"])
            print("Connect to mysql successful.")
        except Exception as e:
            print("Connect to mysql failed.")
            print(str(e))
        finally:
            if "conn" in locals().keys():
                return conn
            else:
                return ""

    def connect_to_hbase(self, hbase_info={}):

        # thrift默认端口是9090
        socket = TSocket.TSocket(hbase_info["host"], hbase_info["port"])
        socket.setTimeout(5000)

        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        client = Hbase.Client(protocol)
        socket.open()

        return client
        # print(client.getTableNames())  # 获取当前所有的表名

    def connect_to_oracle(self):
        conn = cx_Oracle.connect("hs_com", "jyhTEST123", "192.168.44.172:1521/JYHTestDB")
        cursor = conn.cursor()
        while True:
            with open("print.txt", "w")as f:
                sql = "SELECT INIT_DATE, ENTRUST_NO, CURR_DATE, BUSINESS_AMOUNT,POST_AMOUNT, BUSINESS_PRICE,BUSIN_ACCOUNT \
                from COMENTRUSTDETAIL where INIT_DATE = '20200519' ORDER BY ENTRUST_NO desc"
                f.write("""{0},{1},{2}""".format("*"*20, "COMENTRUSTDETAIL", "*"*20))
                print("*"*20, "COMENTRUSTDETAIL", "*"*20)
                for each in cursor.execute(sql):
                    f.write(str(each))
                    print(each)
                sql2 = "SELECT INIT_DATE,SERIAL_NO,TAG,PUSH_STATUS,SENDERCOMP_ID from COMPUSH \
        where INIT_DATE = '20200519' ORDER BY SERIAL_NO desc"
                f.write("""{0},{1},{2}""".format("*" * 25, "COMPUSH", "*" * 25))
                print("*" * 25, "COMPUSH", "*" * 25)
                for each in cursor.execute(sql2):
                    f.write(str(each))
                    print(each)
                time.sleep(0.1)
        return


def test_normal(cur):
    sql = "SELECT * from interval_trade_style where fund_account = 11000619 and interval_type = 1;"
    origin_data = {}
    print("Will execute sql: \n", sql)
    cur.execute(sql)
    for each in cur.fetchone():
        print(each, type(each))
        if isinstance(each, decimal.Decimal):
            print("*"*10)

get_configurations = GetConfigurations()
hbase_client = get_configurations.connect_to_hbase(get_configurations.get_target_section(section="database_chasing"))
if __name__ == "__main__":
    init_date = str(99999999 - 20200326)
    # print(hbase_client.send_getRowWithColumns("chenk_zhfx:bond_page_user_daily_data", "88888,{0}".format(init_date), "tag_base:init_date"))
    # print(hbase_client.isTableEnabled("chenk_zhfx:bond_page_user_daily_data"))
    # print(hbase_client.getTableNames())
    # print(hbase_client.getRow("chenk_zhfx:stock_page_user_daily_data", "00988,79799569"))
    # print(hbase_client.send_mutateRows("chenk_zhfx:stock_page_user_daily_data", "00988,79799569"))
    # print(hbase_client.mutateRows("chenk_zhfx:stock_page_user_daily_data", "00988,79799569"))
    get_configurations.connect_to_oracle()

    # conn = get_configurations.connect_to_mysql(get_configurations.get_target_section(section="database"))
    # test_normal(conn.cursor())
    # print(type("jkl"), isinstance("asdjkl", str))
    # print(get_configurations.get_target_section(section='database'))