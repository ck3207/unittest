import random
import datetime


class CaifuAnalysis:
    def __init__(self):
        self.start_fund_account = 88888
        self.fund_account = 20
        self.init_date_special_deal_num = 99999999
        self.start_init_date = "20190502"
        self.init_date_num = 365
        self.interval_types = ["1", "2", "3", "4"]
        self.f = ""
        
    def market_cumulative_data(self):        
        sql = "insert into market_cumulative_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5}),"
        
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') + datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')

            for interval_type in self.interval_types:
                sql += sql_model.format(",".join([interval_type, start_init_date]), start_init_date, self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0), self.get_random_num(1, 4, 0))
        self.f.write(sql[:-1]+";\n\n")
        return
    
    def daily_basic_data(self):
        sql = "insert into daily_basic_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},{9}),"
        base_price = 3000
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') + datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')

            sql += sql_model.format(start_init_date, start_init_date, self.get_random_num(300, 2, 0, base_price), self.get_random_num(1, 4, 0), self.get_random_num(300, 2, 0, base_price), self.get_random_num(1, 4, 0), self.get_random_num(300, 2, 0, base_price), self.get_random_num(1, 4, 0),
            self.get_random_num(300, 2, 0, base_price), self.get_random_num(1, 4, 0))
        self.f.write(sql[:-1]+";\n\n")
        return
        
    def home_page_user_daily_data(self):
        sql = "insert into home_page_user_daily_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}),"
        base_price = 3000
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') + datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                sql += sql_model.format(",".join([fund_account, str(self.init_date_special_deal_num-int(start_init_date))]), start_init_date, self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(1, 4, 0), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1), self.get_random_num(123456, 2, 1))
        self.f.write(sql[:-1]+";\n\n")
        return

    def home_page_user_interval_data(self):
        sql = "insert into home_page_user_interval_data values"
        sql_model = "('{0}','{1}',{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15}),"
        base_price = 3000
        asset_prop = "0"
        for init_date_num in range(self.init_date_num):
            start_init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') + datetime.timedelta(days=init_date_num)).strftime('%Y%m%d')
            for fund_account_num in range(self.fund_account):
                fund_account = "".join(reversed(str(self.start_fund_account + fund_account_num)))
                for interval_type in self.interval_types:
                    sql += sql_model.format(",".join([fund_account, interval_type, asset_prop, str(self.init_date_special_deal_num-int(start_init_date))]), self.get_random_num(123456, 2, 0),self.get_random_num(1, 4, 0), self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0), self.get_random_num(123456, 2, 0),self.get_random_num(123456, 2, 0),self.get_random_num(123456, 2, 0),self.get_random_num(1234567, 2, 1), self.get_random_num(123456, 2, 1),self.get_random_num(123456, 2, 1),self.get_random_num(1234567, 2, 1), self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1), self.get_random_num(1, 4, 1))
        self.f.write(sql[:-1]+";\n\n")
        return
        
    def get_random_num(self, base_num, decimal_num, is_gt_0, add_num=0):
        is_gt = random.choice([1, -1])
        if not isinstance(base_num, int) or not isinstance(decimal_num, int):
            return "base_num or decimal_num is not valid"
        if is_gt_0 == 1:
            is_gt = 1
        return round(base_num * random.uniform(0, 1) * is_gt, decimal_num)
    
    def write_to_file(self, filename):
        self.f = open(filename, mode="w", encoding="utf-8")
        
    def close_file(self):
        self.f.close()
            
if __name__ == "__main__":
    caifu_analysis = CaifuAnalysis()
    caifu_analysis.write_to_file("caifu_analysis.sql")
    # caifu_analysis.market_cumulative_data()
    # caifu_analysis.daily_basic_data()
    # caifu_analysis.home_page_user_daily_data()
    caifu_analysis.home_page_user_interval_data()
    caifu_analysis.close_file()
