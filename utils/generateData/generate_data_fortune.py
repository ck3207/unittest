import random
import datetime
import sys


class CaifuAnalysis:
    STOCK_INFO = {"赣锋锂业": "002460", "龙净环保": "600388", "中国平安": "601318",
                  "宁波银行": "002142", "温氏股份": "300498", "海康威视": "002415",
                  "立讯精密": "002475", "中国人寿": "601628", "交通银行": "601328",
                  "中国建筑": "601668", "海天味业": "603288", "同花顺": "300033",
                  "财富宝": "000719"}
    FINANCIAL_INFO = {"华润信托鑫华10号": "T12139", "永安国富-永富18号1期": "SJZ992",
                      "幻方量化专享1号1期": "SGK660", "聚丰3号": "C43350",
                      "月月福23号": "C43394", "润富6号": "C43460", "吉富1号": "C43328",
                      "云溪金牛A": "SCF241", "少数派132号": "SJU205", "私享-星华1号": "SGK077"}
    BOND_INFO = {"国轩转债": "128068", "振德转债": "113555", "博特转债": "113571", "希望转债": "127015",
                 "华夏转债": "128077", "精测转债": "123025", "新泉转债": "113509", "唐人转债": "128092",
                 "环境转债": "113028", "中宠转债": "128054", "至纯转债": "113556"}
    # BUSINESS_FLAG = {"卖出":"4001", "买入": "4002", "分红": "4016", "股息入账": "4015"}
    BUSINESS_FLAG = {"卖出":"4001", "买入": "4002", "分红": "4016"}

    def __init__(self):
        self.start_fund_account = 88888
        self.fund_account = 20
        self.init_date_special_deal_num = 99999999
        self.start_init_date = "20190502"
        self.init_date_num = 365
        self.interval_types = ["1", ]
        self.f = ""
        
    def market_cumulative_data(self):        
        sql = "insert into market_cumulative_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5}),"
        
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')

            for interval_type in self.interval_types:
                sql += sql_model.format(",".join([interval_type, start_init_date]), start_init_date,
                                        self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0),
                                        self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0))
        self.f.write(sql[:-1]+";\n\n")
        return
    
    def daily_basic_data(self):
        sql = "insert into daily_basic_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},{9}),"
        base_price = 3000
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')

            sql += sql_model.format(str(self.init_date_special_deal_num-int(start_init_date)),
                                    start_init_date, self.get_random_num(300, 2, 0, base_price),
                                    self.get_random_num(1, 4, 0), self.get_random_num(300, 2, 0, base_price),
                                    self.get_random_num(1, 4, 0), self.get_random_num(300, 2, 0, base_price),
                                    self.get_random_num(1, 4, 0), self.get_random_num(300, 2, 0, base_price),
                                    self.get_random_num(1, 4, 0))
        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-1]+";\n\n")
        return
        
    def home_page_user_daily_data(self):
        sql = "insert into home_page_user_daily_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}),"
        base_price = 3000
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                sql += sql_model.format(",".join([fund_account,
                                                  str(self.init_date_special_deal_num-int(start_init_date))]),
                                        start_init_date, self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(1, 4, 0),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1))
        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-1]+";\n\n")
        return

    def home_page_user_interval_data(self):
        sql = "insert into home_page_user_interval_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15}),"
        base_price = 3000
        asset_prop = "0"
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                for interval_type in self.interval_types:
                    sql += sql_model.format(",".join([fund_account, interval_type, asset_prop,
                                                      str(self.init_date_special_deal_num-int(start_init_date))]),
                                            self.get_random_num(123456, 2, 0),self.get_random_num(1, 4, 0),
                                            self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0),
                                            self.get_random_num(123456, 2, 0),self.get_random_num(123456, 2, 0),
                                            self.get_random_num(123456, 2, 0),self.get_random_num(1234567, 2, 1),
                                            self.get_random_num(123456, 2, 1),self.get_random_num(123456, 2, 1),
                                            self.get_random_num(1234567, 2, 1), self.get_random_num(1, 4, 1),
                                            self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                            self.get_random_num(1, 4, 1))
        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-1]+";\n\n")
        return

    def home_page_user_month_data(self):
        sql = "insert into home_page_user_month_data values"
        sql_model = "('{0}','{1}',{2},{3}),"
        for month_delay in range(12):
            init_month = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=month_delay*30)).strftime('%Y%m')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                sql += sql_model.format(",".join([fund_account, init_month]), init_month,
                                        self.get_random_num(123456, 2, 0), self.get_random_num(1, 4, 0))
        self.f.write(sql[:-1]+";\n\n")
        return

    def cash_page_user_daily_data(self):
        sql = "insert into cash_page_user_daily_data values"
        sql_model = "('{0}','{1}',{2},{3},{4}),"
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1))
        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-1]+";\n\n")
        return

    def fund_price_data(self):
        sql = "insert into fund_price_data values"
        sql_model = """('{0}',"{1}"),\n"""

        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')

            for stock_name, stock_code in CaifuAnalysis.STOCK_INFO.items():
                stock_hold_data = []
                element = {"init_date": init_date,
                           "per_myriad_income": self.get_random_num(123456, 2, 0),
                           "seven_income_ratio": self.get_random_num(1, 4, 0),
                           "product_no": stock_code}
                stock_hold_data.append(element)
                sql += sql_model.format(stock_code, """{}""".format(stock_hold_data))

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def stock_detail_page_price_data(self):
        sql = "insert into stock_detail_page_price_data values"
        sql_model = """('{0}',"{1}"),\n"""

        for stock_name, stock_code in CaifuAnalysis.STOCK_INFO.items():
            stock_hold_data = []
            for init_date_delay in range(self.init_date_num):
                init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                             datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
                exchange_type = "1"
                if stock_code.startswith("0"):
                    exchange_type = "2"
                element = {"init_date": init_date,
                           "min_time": init_date,
                           "open_px": self.get_random_num(10, 2, 0, 100),
                           "high_px": self.get_random_num(10, 2, 1, 110),
                           "low_px": self.get_random_num(-10, 2, 1, 90),
                           "close_px": self.get_random_num(10, 2, 0, 100)}
                stock_hold_data.append(element)
            sql += sql_model.format(",".join([stock_code, exchange_type]),
                                    "{}".format(stock_hold_data))

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def stock_page_user_daily_data(self):
        sql = "insert into stock_page_user_daily_data values"
        sql_model = """('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},"{9}"),\n"""
        base_num = 1000000
        for init_date_delay in range(0, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                stock_hold_data = self.__get_stock_code_info_list(stock_page_user_daily_data=1)
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(12345678, 2, 1),
                                        self.get_random_num(123456, 2, 0), self.get_random_num(1, 4, 0),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(1, 4, 1),
                                        stock_hold_data)
        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def stock_page_user_interval_data(self):
        sql = "insert into stock_page_user_interval_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},'{6}','{7}',{8},'{9}','{10}',{11},{12},{13},{14},{15},{16},{17},{18},{19}," \
                    "{20},{21},{22},{23},{24},{25}),\n"
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    sql += sql_model.format(",".join([fund_account, interval_type, "0",
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            self.get_random_num(123456, 2, 1), self.get_random_num(123000, 1, 1),
                                            self.get_random_num(1000, 2, 1), self.get_random_num(1, 4, 1),
                                            "600000", "浦发银行", self.get_random_num(123456, 2, 1),
                                            "000001", "平安银行", -self.get_random_num(123456, 2, 1),
                                            self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(200, 2, 1), self.get_random_num(200, 2, 1),
                                            self.get_random_num(200, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(123456, 2, 1),
                                            self.get_random_num(123456, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 4, 1),
                                            self.get_random_num(1000, 2, 1)
                                            )

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def stock_page_user_single_stock_interval_data(self):
        sql = "insert into stock_page_user_single_stock_interval_data values"
        sql_model = """('{0}',"{1}"),\n"""
        for init_date_delay in range(100, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    json_content = self.__get_stock_code_info_list(stock_page_user_single_stock_interval_data=1)
                    sql += sql_model.format(",".join([fund_account, interval_type,
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            json_content)

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def stock_page_user_interval_trade_distribution(self):
        sql = "insert into stock_page_user_interval_trade_distribution values"
        sql_model = """('{0}',"{1}"),\n"""
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    json_content = self.__get_stock_code_info_list(stock_page_user_interval_trade_distribution=1)
                    sql += sql_model.format(",".join([fund_account, interval_type,
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            json_content)

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def financial_page_user_daily_data(self):
        sql = "insert into financial_page_user_daily_data values"
        sql_model = """('{0}',"{1}",{2},{3},"{4}"),\n"""
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                json_content = self.__get_stock_code_info_list(financial_page_user_daily_data=1, init_date=init_date)
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(1234567, 2, 1),
                                        self.get_random_num(123456, 2, 1), json_content)

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def financial_page_user_interval_data(self):
        sql = "insert into financial_page_user_interval_data values"
        sql_model = """('{0}',{1},{2},{3},'{4}','{5}',{6},'{7}','{8}',{9},{10},{11},"{12}"),\n"""
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    json_content = self.__get_stock_code_info_list(financial_page_user_interval_data=1, init_date=init_date)
                    sql += sql_model.format(",".join([fund_account, interval_type, "0",
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            self.get_random_num(123456, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), "T12139", "华润信托鑫华10号",
                                            self.get_random_num(123456, 2, 1), "SGK660", "幻方量化专享1号1期",
                                            self.get_random_num(123456, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), json_content)
        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def bond_page_user_daily_data(self):
        sql = "insert into bond_page_user_daily_data values"
        sql_model = """('{0}','{1}',{2},{3},"{4}"),\n"""
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                json_content = self.__get_stock_code_info_list(bond_page_user_daily_data=1, init_date=init_date)
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(12345678, 2, 1),
                                        self.get_random_num(123456, 2, 1), json_content)

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def bond_page_user_interval_data(self):
        sql = "insert into bond_page_user_interval_data values"
        sql_model = """('{0}',{1},{2},{3},{4},{5},'{6}','{7}',{8},'{9}','{10}',{11},{12},{13},{14},{15},{16},"{17}"),\n"""
        for init_date_delay in range(0, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    json_content = self.__get_stock_code_info_list(bond_page_user_interval_data=1, init_date=init_date)
                    sql += sql_model.format(",".join([fund_account, interval_type, "0",
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            self.get_random_num(123456, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(1, 4, 1),
                                            "128068", "国轩转债", self.get_random_num(123456, 2, 1),
                                            "113028", "环境转债", -self.get_random_num(123456, 2, 1),
                                            self.get_random_num(12345678, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            json_content)

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def cfb_page_user_daily_data(self):
        sql = "insert into cfb_page_user_daily_data values"
        sql_model = """('{0}','{1}',{2},{3},{4}),\n"""
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(12345678, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1))

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def new_stock_page_data(self):
        sql = "insert into new_stock_page_data values"
        sql_model = """('{0}',"{1}","{2}"),\n"""
        for init_date_delay in range(0, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    stock_content = self.__get_stock_code_info_list(new_stock_page_data=1, stock_content=1,
                                                                    init_date=init_date)
                    bond_content = self.__get_stock_code_info_list(new_stock_page_data=1, bond_content=1,
                                                                   init_date=init_date)
                    sql += sql_model.format(",".join([fund_account, interval_type, "0",
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            stock_content, bond_content)

        self.f.write("use wt_hbase_chenk;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def stock_detail_page_data(self):
        sql = "insert into stock_detail_page_data values"
        sql_model = """('{0}',"{1}"),\n"""
        yield_init_date = self.yield_init_date(start=0, end=self.init_date_num, step=2)

        while True:
            try:
                init_date = yield_init_date.__next__()
            except StopIteration:
                self.f.write("use wt_hbase_chenk;\n")
                self.f.write(sql[:-2] + ";\n\n")
                return
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for stock_name, stock_code in CaifuAnalysis.STOCK_INFO.items():
                    exchange_type = "2"
                    if stock_code.startswith("6"):
                        exchange_type = "1"
                    json_content = self.__get_stock_code_info_list(stock_detail_page_data=1, init_date=init_date)
                    sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date)),
                                                      stock_code, exchange_type]), json_content)

    def get_random_num(self, base_num, decimal_num, is_gt_0, add_num=0):
        is_gt = random.choice([1, -1])
        if not isinstance(base_num, int) or not isinstance(decimal_num, int):
            return "base_num or decimal_num is not valid"
        if is_gt_0 == 1:
            is_gt = 1
        return round(base_num * random.uniform(0, 1) * is_gt + add_num, decimal_num)
    
    def write_to_file(self, filename):
        self.f = open(filename, mode="w", encoding="utf-8")
        
    def close_file(self):
        self.f.close()

    def __get_stock_code_info_list(self, **kwargs):
        # stock_hold_data 股票持仓信息数据
        distribute_info = {"0": ["金融", "银行", "地产", "大数据", "5G", "人工智能", "环保"],
                             "1": ["小盘股", "中盘股", "大盘股"],
                             "2": ["上证", "深证", "新三板", "港股通", "其他"]}
        stock_hold_data = []
        base_num = 1000000
        if kwargs.get("stock_page_user_daily_data") or kwargs.get("stock_page_user_single_stock_interval_data") :
            for stock_name, stock_code in CaifuAnalysis.STOCK_INFO.items():
                exchange_type = "2"
                market_suffix = ".SZ"
                if stock_code.startswith("6"):
                    exchange_type = "1"
                    market_suffix = ".SS"
                if kwargs.get("stock_page_user_daily_data"):
                    element = {"stock_code": stock_code, "stock_name": stock_name, "exchange_type": exchange_type,
                               "stock_market_value": base_num + self.get_random_num(123456, 2, 1),
                               "single_stock_day_income": self.get_random_num(12345, 2, 1),
                               "single_stock_day_income_ratio": self.get_random_num(1, 4, 0),
                               "hold_amount": self.get_random_num(123000, 0, 1),
                               "cost_price": self.get_random_num(100, 4, 1),
                               "asset_price": self.get_random_num(100, 4, 1)}
                elif kwargs.get("stock_page_user_single_stock_interval_data"):
                    element = {"stock_code": stock_code, "stock_name": stock_name,
                               "income": self.get_random_num(123456, 2, 0),
                               "hold_days": self.get_random_num(100, 2, 1),
                               "hold_status": random.choice([0, 1]),
                               "exchange_type": exchange_type, "market_suffix": market_suffix}
                stock_hold_data.append(element)
        elif kwargs.get("stock_page_user_interval_trade_distribution"):
            for distribute_type, distribute_names in distribute_info.items():
                value_sum = 0
                for i, distribute_name in enumerate(distribute_names):
                    if i == len(distribute_names) - 1:
                        value = 1 - value_sum
                    else:
                        value = (1-value_sum) * self.get_random_num(1, 4, 1)
                    value_sum += value
                    element = {"distribute_type": distribute_type, "distribute_name": distribute_name,
                               "distribute_value": value}
                    stock_hold_data.append(element)
                    if value_sum >= 1:
                        break
        elif kwargs.get("financial_page_user_daily_data") or kwargs.get("financial_page_user_interval_data"):
            for prod_name, prod_code in CaifuAnalysis.FINANCIAL_INFO.items():
                if kwargs.get("financial_page_user_daily_data"):
                    element = {"prod_code": prod_code, "prod_name": prod_name,
                               "financial_market_value": base_num + self.get_random_num(123456, 2, 1),
                               "single_financial_day_income": self.get_random_num(123456, 2, 1),
                               "end_date": kwargs.get("init_date")}
                elif kwargs.get("financial_page_user_interval_data"):
                    element = {"prod_code": prod_code, "prod_name": prod_name,
                               "cumu_income": self.get_random_num(123456, 2, 1),
                               "hold_days": self.get_random_num(100, 2, 1),
                               "end_date": kwargs.get("init_date"),
                               "hold_status": random.choice([0, 1])}
                stock_hold_data.append(element)
        elif kwargs.get("bond_page_user_daily_data") or kwargs.get("bond_page_user_interval_data"):

            for stock_name, stock_code in CaifuAnalysis.BOND_INFO.items():
                exchange_type = "1"
                if stock_code.startswith("11"):
                    exchange_type = "0"
                if kwargs.get("bond_page_user_daily_data"):
                    element = {"stock_code": stock_code, "stock_name": stock_name, "exchange_type": exchange_type,
                               "single_bond_day_income": self.get_random_num(123456, 2, 1),
                               "bond_market_value": self.get_random_num(123456, 2, 1),
                               "bond_nums": self.get_random_num(100, 0, 1)}
                elif kwargs.get("bond_page_user_interval_data"):
                    element = {"stock_code": stock_code, "stock_name": stock_name, "exchange_type": exchange_type,
                               "cumu_income": self.get_random_num(123456, 2, 1),
                               "hold_days": self.get_random_num(100, 2, 1),
                               "bond_market_value": self.get_random_num(123456, 2, 1),
                               "end_date": kwargs.get("init_date")}
                stock_hold_data.append(element)
        elif kwargs.get("new_stock_page_data"):
            if kwargs.get("stock_content"):
                for stock_name, stock_code in CaifuAnalysis.STOCK_INFO.items():
                    element = {"stock_name": stock_name, "stock_code": stock_code,
                               "init_date": kwargs.get("init_date"),
                               "business_amount": self.get_random_num(1000, 2, 1),
                               "first_up_rate": self.get_random_num(10, 4, 1),
                               "total_up_rate": self.get_random_num(20, 4, 1)}
                    stock_hold_data.append(element)
            elif kwargs.get("bond_content"):
                for stock_name, stock_code in CaifuAnalysis.BOND_INFO.items():
                    element = {"stock_name": stock_name, "stock_code": stock_code,
                               "init_date": kwargs.get("init_date"),
                               "business_amount": self.get_random_num(1000, 2, 1),
                               "first_up_rate": self.get_random_num(10, 4, 1),
                               "total_up_rate": self.get_random_num(20, 4, 1)}
                    stock_hold_data.append(element)
        elif kwargs.get("stock_detail_page_data"):
            business_name = random.choice(list(CaifuAnalysis.BUSINESS_FLAG.keys()))
            element = {"init_date": kwargs.get("init_date"),
                       "business_time": "{2}{0}{1}{1}".format(random.randint(10, 11), random.randint(10, 60),
                                                                kwargs.get("init_date")),
                       "serial_no": 10000 + random.randint(1000, 9999),
                       "business_flag": CaifuAnalysis.BUSINESS_FLAG.get(business_name),
                       "business_amount": self.get_random_num(123000, 2, 1),
                       "business_price": self.get_random_num(100, 4, 1),
                       "business_name": business_name,
                       "business_balance": self.get_random_num(123456, 2, 1)}
            stock_hold_data.append(element)

        return "{}".format(stock_hold_data)

    def yield_init_date(self, start=0, end=0, step=10):
        for init_date_delay in range(start, end, step):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            # print(init_date)
            yield init_date
            
if __name__ == "__main__":
    caifu_analysis = CaifuAnalysis()
    caifu_analysis.write_to_file("caifu_analysis.sql")
    # caifu_analysis.market_cumulative_data()
    # caifu_analysis.daily_basic_data()
    # caifu_analysis.home_page_user_daily_data()
    # caifu_analysis.home_page_user_interval_data()
    # caifu_analysis.home_page_user_month_data()
    # caifu_analysis.cash_page_user_daily_data()
    # caifu_analysis.fund_price_data()
    caifu_analysis.stock_detail_page_price_data()
    # caifu_analysis.stock_page_user_daily_data()
    # caifu_analysis.stock_page_user_interval_data()
    # caifu_analysis.stock_page_user_single_stock_interval_data()
    # caifu_analysis.stock_page_user_interval_trade_distribution()
    # caifu_analysis.financial_page_user_daily_data()
    # caifu_analysis.financial_page_user_interval_data()
    # caifu_analysis.bond_page_user_daily_data()
    # caifu_analysis.bond_page_user_interval_data()
    # caifu_analysis.cfb_page_user_daily_data()
    # caifu_analysis.new_stock_page_data()
    # caifu_analysis.stock_detail_page_data()
    caifu_analysis.close_file()
