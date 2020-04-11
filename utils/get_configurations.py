# -*- coding: utf-8 -*-
import configparser
import pymysql
import decimal
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
# conn = get_configurations.connect_to_mysql(get_configurations.get_target_section(section="database"))
# test_normal(conn.cursor())
# print(type("jkl"), isinstance("asdjkl", str))
# print(get_configurations.get_target_section(section='database'))