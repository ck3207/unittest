import random
import datetime
import sys

from deal_data_from_create_sql_file import sqls_info

class GuoLianAnalysis:
    STOCK_INFO = {"赣锋锂业": "002460", "龙净环保": "600388", "中国平安": "601318",
                  "宁波银行": "002142", "温氏股份": "300498", "海康威视": "002415",
                  "立讯精密": "002475", "中国人寿": "601628", "交通银行": "601328",
                  "中国建筑": "601668", "海天味业": "603288", "同花顺": "300033"}
    FINANCIAL_INFO = {"华润信托鑫华10号": "T12139", "永安国富-永富18号1期": "SJZ992",
                      "幻方量化专享1号1期": "SGK660", "聚丰3号": "C43350",
                      "月月福23号": "C43394", "润富6号": "C43460", "吉富1号": "C43328",
                      "云溪金牛A": "SCF241", "少数派132号": "SJU205", "私享-星华1号": "SGK077"}
    BOND_INFO = {"国轩转债": "128068", "振德转债": "113555", "博特转债": "113571", "希望转债": "127015",
                 "华夏转债": "128077", "精测转债": "123025", "新泉转债": "113509", "唐人转债": "128092",
                 "环境转债": "113028", "中宠转债": "128054", "至纯转债": "113556"}
    BUSINESS_FLAG = {"证券卖出":"4001", "证券买入": "4002", "红股入账": "4016", "股息入账": "4015"}

    def __init__(self):
        self.start_fund_account = 777700
        self.fund_account = 1
        self.init_date_special_deal_num = 99999999
        self.start_init_date = "20190502"
        self.init_date_num = 365
        self.interval_types = ["1", "2", "3", "4"]
        self.f = ""

    def cal_record(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = "({0}),\n".format(sqls_info.get(_table_name))
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            sql += sql_model.format(start_init_date, start_init_date)
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def basic_data(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = "({0}),\n".format(sqls_info.get(_table_name))
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for interval_type in ["1", "2", "3", "4"]:
                sql += sql_model.format(",".join([interval_type,
                                                  str(self.init_date_special_deal_num-int(start_init_date))]),
                                        start_init_date, self.get_random_num(1, 4, 0),
                                        self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0),
                                        self.get_random_num(1, 4, 0), self.get_random_num(100, 2, 1),
                                        self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                        self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                        self.get_random_num(100, 2, 1))
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def credit_basic_data(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = "({0}),\n".format(sqls_info.get(_table_name))
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for interval_type in ["1", "2", "3", "4"]:
                sql += sql_model.format(",".join([interval_type,
                                                  str(self.init_date_special_deal_num-int(start_init_date))]),
                                        start_init_date, self.get_random_num(1, 4, 0),
                                        self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0),
                                        self.get_random_num(1, 4, 0), self.get_random_num(100, 2, 1),
                                        self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                        self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                        self.get_random_num(100, 2, 1))
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def daily_basic_data(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = "({0}),\n".format(sqls_info.get(_table_name))
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            sql += sql_model.format(",".join([str(self.init_date_special_deal_num-int(start_init_date))]),
                                    start_init_date, self.get_random_num(100, 2, 1, 3000),
                                    self.get_random_num(1, 4, 0), self.get_random_num(100, 2, 1, 9000),
                                    self.get_random_num(1, 4, 0), self.get_random_num(100, 2, 1, 2000),
                                    self.get_random_num(1, 4, 0), self.get_random_num(100, 2, 1, 4000),
                                    self.get_random_num(1, 4, 0))
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def interval_stock(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = "({0}),\n".format(sqls_info.get(_table_name))
        asset_prop = "0"
        for init_date_num in range(180, self.init_date_num, 1):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                if fund_account_num % 2 == 0:
                    trade_type = "0"
                else:
                    trade_type = "1"
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                for interval_type in self.interval_types:
                    stock_content = self.__get_stock_code_info_list(interval_stock=1)
                    sql += sql_model.format(",".join([fund_account, interval_type, trade_type,
                                                      str(self.init_date_special_deal_num-int(start_init_date))]),
                                            stock_content)
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def interval_fund_rank(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = "({0}),\n".format(sqls_info.get(_table_name))
        for init_date_num in range(0, self.init_date_num, 1):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                               datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            elements = []
            for interval_type in self.interval_types:
                for stock_name, stock_code in GuoLianAnalysis.FINANCIAL_INFO.items():
                    element = {"prod_no": stock_name, "prod_code": stock_code, "yield": str(self.get_random_num(1, 4, 0))}
                    elements.append(element)
                sql += sql_model.format(",".join([interval_type,
                                                  str(self.init_date_special_deal_num-int(start_init_date))]), elements)
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def interval_trade_distribution(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(150, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                if fund_account_num % 2 == 0:
                    trade_type = "0"
                else:
                    trade_type = "1"
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))

                for interval_type in self.interval_types:
                    distribute_content = self.__get_stock_code_info_list(interval_trade_distribution=1)
                    sql += sql_model.format(",".join([fund_account, interval_type, trade_type,
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            distribute_content)

        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def user_daily_asset(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(12345678, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(12345678, 2, 1),
                                        self.get_random_num(12345678, 2, 1), self.get_random_num(12345678, 2, 1),
                                        self.get_random_num(12345678, 2, 1))

        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def user_daily_data(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                single_stock_content = self.__get_stock_code_info_list(user_daily_data=1, single_stock_content=1)
                deliver_content = self.__get_stock_code_info_list(user_daily_data=_table_name, deliver_content=1,
                                                                  init_date=init_date)
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(123456, 2, 0), self.get_random_num(1, 4, 0),
                                        self.get_random_num(12345678, 2, 1), self.get_random_num(123456, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(1, 4, 0),
                                        self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                        self.get_random_num(123456, 2, 1), single_stock_content, deliver_content)

        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def his_deliver(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                for stock_name, stock_code in GuoLianAnalysis.STOCK_INFO.items():
                    exchange_type = "1"
                    if stock_code.startswith("6"):
                        exchange_type = "0"
                    fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                    deliver_content = self.__get_stock_code_info_list(his_deliver=_table_name, deliver_content=1,
                                                                      init_date=init_date)
                    sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date)),
                                                      stock_code, exchange_type]), deliver_content)

        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def credit_user_daily_data(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(self.init_date_num):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(init_date))]),
                                        init_date, self.get_random_num(1234567, 2, 1),
                                        self.get_random_num(123456, 2, 1), self.get_random_num(12345, 2, 0),
                                        self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 1, 1),
                                        self.get_random_num(1, 4, 0))

        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def home_page_data(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(0, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                if int(self.start_fund_account + fund_account_num) % 2 == 0:
                    asset_prop = "0"
                else:
                    asset_prop = "7"
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                for interval_type in self.interval_types:
                    begin_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=-init_date_delay*30-30)).strftime('%Y%m%d')
                    profits = {}
                    for i in range(6):
                        stock_name = random.choice(list(GuoLianAnalysis.STOCK_INFO.keys()))
                        stock_code = GuoLianAnalysis.STOCK_INFO.get(stock_name)
                        exchange_type = "2"
                        if stock_code.startswith("6"):
                            exchange_type = "1"
                        if i < 3:
                            is_gt_0 = 1
                        else:
                            is_gt_0 = -1
                        profits.setdefault(i, {"stock_name": stock_name, "stock_code": stock_code,
                                               "income": str(self.get_random_num(123456, 2, 1) * is_gt_0),
                                               "income_rate": str(self.get_random_num(1, 4, 1) * is_gt_0),
                                               "trade_type": str(random.choice([0, 1])),
                                               })

                    sql += sql_model.format(",".join([fund_account, interval_type, asset_prop,
                                                      str(self.init_date_special_deal_num-int(init_date))]),
                                            self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0),
                                            self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0),
                                            self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0),
                                            self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0),
                                            self.get_random_num(1234, 2, 0), self.get_random_num(1, 4, 0),
                                            self.get_random_num(1, 4, 0), self.get_random_num(123456, 2, 1),
                                            self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                            self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 1),
                                            self.get_random_num(1000, 2, 1), self.get_random_num(1000, 2, 1),
                                            self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            begin_date, self.get_random_num(12345678, 2, 1),
                                            self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1),
                                            self.get_random_num(12345678, 2, 1), init_date,
                                            self.get_random_num(1, 4, 0), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(1, 4, 1),
                                            self.get_random_num(1, 4, 1), profits.get(0), profits.get(1),
                                            profits.get(2), profits.get(3), profits.get(4), profits.get(5),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                            self.get_random_num(1, 4, 1), random.choice([0, 1]))

        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def trade_statistics(self):
        _table_name = sys._getframe().f_code.co_name
        sql = "insert into {0} values".format(_table_name)
        sql_model = """({0}),\n""".format(sqls_info.get(_table_name))
        for init_date_delay in range(180, self.init_date_num, 1):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                for interval_type in self.interval_types:
                    for stock_name, stock_code in GuoLianAnalysis.STOCK_INFO.items():
                        exchange_type = "2"
                        if stock_code.startswith("6"):
                            exchange_type = "1"
                        sql += sql_model.format(",".join([fund_account, interval_type, random.choice(["0", "1"]),
                                                          str(self.init_date_special_deal_num-int(init_date))]),
                                                stock_name, stock_code, "0", exchange_type,
                                                self.get_random_num(100, 0, 1), self.get_random_num(123456, 2, 0),
                                                self.get_random_num(1234567, 2, 1), self.get_random_num(100, 0, 1),
                                                self.get_random_num(1000, 0, 1), self.get_random_num(1000, 0, 1),
                                                self.get_random_num(100, 2, 1), self.get_random_num(100, 2, 1),
                                                self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                                self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                                self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                                self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1),
                                                self.get_random_num(1, 4, 1))
        self.f.write("use wt_hbase_chenk_gl;\n")
        self.f.write(sql[:-2]+";\n\n")
        return

    def get_random_num(self, base_num, decimal_num, is_gt_0, add_num=0):
        is_gt = random.choice([1, -1])
        if not isinstance(base_num, int) or not isinstance(decimal_num, int):
            return "base_num or decimal_num is not valid"
        if is_gt_0 == 1:
            is_gt = 1
        return round(base_num * random.uniform(0, 1) * is_gt, decimal_num) + add_num
    
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
        if kwargs.get("user_daily_data") or kwargs.get("stock_page_user_single_stock_interval_data"):
            for stock_name, stock_code in GuoLianAnalysis.STOCK_INFO.items():
                exchange_type = "1"
                if stock_code.startswith("6"):
                    exchange_type = "0"
                if kwargs.get("user_daily_data") and kwargs.get("single_stock_content"):
                    element = {"stock_code": stock_code, "stock_name": stock_name, "exchange_type": exchange_type,
                               "stock_market_value": base_num + self.get_random_num(123456, 2, 1),
                               "single_stock_day_income": self.get_random_num(12345, 2, 1),
                               "business_status": random.choice([1, 2, 3, 4]),  # 1建仓，2加仓，3减仓，4清仓
                               "single_stock_day_ratio": self.get_random_num(1, 4, 0),
                               "asset_price": self.get_random_num(100, 4, 1)}
                elif kwargs.get("user_daily_data") and kwargs.get("deliver_content"):
                    business_name = random.choice(list(GuoLianAnalysis.BUSINESS_FLAG.keys()))
                    element = {"stock_code": stock_code, "stock_name": stock_name, "exchange_type": exchange_type,
                               "init_date": kwargs.get("init_date"),
                               "business_time": "{0}{1}{1}".format(random.randint(9, 11), random.randint(0, 60)),
                               "business_flag": GuoLianAnalysis.BUSINESS_FLAG.get(business_name),
                               "business_amount": self.get_random_num(123000, 2, 1),
                               "business_price": self.get_random_num(100, 4, 1),
                               "business_status": random.choice([1, 2, 3, 4]),  # 1建仓，2加仓，3减仓，4清仓
                               "business_balance": self.get_random_num(123456, 2, 0)}
                elif kwargs.get("stock_page_user_single_stock_interval_data"):
                    element = {"stock_code": stock_code, "stock_name": stock_name, "exchange_type": exchange_type,
                               "income": self.get_random_num(123456, 2, 1),
                               "hold_days": self.get_random_num(100, 2, 1),
                               "hold_status": random.choice([0, 1])}
                stock_hold_data.append(element)
        elif kwargs.get("his_deliver") and kwargs.get("deliver_content"):
            business_name = random.choice(list(GuoLianAnalysis.BUSINESS_FLAG.keys()))
            element = {
                       "init_date": kwargs.get("init_date"),
                       "business_time": "{0}{1}{1}".format(random.randint(9, 11), random.randint(0, 60)),
                       "serial_no": "{0}".format(int(base_num + self.get_random_num(12345, 0, 1))),
                       "business_flag": GuoLianAnalysis.BUSINESS_FLAG.get(business_name),
                       "business_amount": int(self.get_random_num(1230, 0, 1)),
                       "post_amount": int(self.get_random_num(1230, 0, 1)),
                       "business_price": self.get_random_num(100, 2, 1),
                       "business_balance": self.get_random_num(123456, 2, 0),
                       "money_type": "0"}
            stock_hold_data.append(element)
        elif kwargs.get("interval_trade_distribution"):
            for distribute_type, distribute_names in distribute_info.items():
                value_sum = 0
                for i, distribute_name in enumerate(distribute_names):
                    if i == len(distribute_names):
                        value = 1 - value_sum
                    else:
                        value = (1-value_sum) * self.get_random_num(1, 4, 1)
                    value_sum += value
                    element = {"distribute_name": distribute_name, "distribute_type": distribute_type,
                               "distribute_mode": random.choice(["1", "2"]), "distribute_value": value}
                    stock_hold_data.append(element)
                    if value_sum >= 1:
                        break
        elif kwargs.get("financial_page_user_daily_data") or kwargs.get("financial_page_user_interval_data"):
            for prod_name, prod_code in GuoLianAnalysis.FINANCIAL_INFO.items():
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
        elif kwargs.get("interval_stock"):
            for stock_name, stock_code in GuoLianAnalysis.STOCK_INFO.items():
                exchange_type = "2"
                if stock_code.startswith("6"):
                    exchange_type = "1"
                element = {"stock_code": stock_code, "stock_name": stock_name, "stock_type": random.choice(["0", "1"]),
                           "exchange_type": exchange_type,
                           "income": self.get_random_num(123456, 2, 1),
                           "hold_day": self.get_random_num(100, 2, 1),
                           "amount": self.get_random_num(100, 0, 1),
                           "hold_status": random.choice(["0", "1"]),
                           "money_type": "0"}
                stock_hold_data.append(element)
        return "{}".format(stock_hold_data)

    def yield_init_date(self, start=0, end=0, step=10):
        for init_date_delay in range(start, end, step):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                          datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            # print(init_date)
            yield init_date
            
if __name__ == "__main__":
    guolian_analysis = GuoLianAnalysis()
    guolian_analysis.write_to_file("guolian_analysis.sql")
    # guolian_analysis.user_daily_asset()
    # guolian_analysis.user_daily_data()
    guolian_analysis.his_deliver()
    # guolian_analysis.credit_user_daily_data()
    # guolian_analysis.basic_data()
    # guolian_analysis.credit_basic_data()
    # guolian_analysis.daily_basic_data()
    # guolian_analysis.home_page_data()
    # guolian_analysis.interval_trade_distribution()
    # guolian_analysis.trade_statistics()
    # guolian_analysis.interval_stock()
    # guolian_analysis.interval_fund_rank()
    # guolian_analysis.cal_record()

    guolian_analysis.close_file()
