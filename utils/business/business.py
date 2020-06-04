import decimal
import json
import datetime
import calendar

class CumulativeRate:

    def cal_cumulative_rate(self, data=[], rate_indexes={0: 4}, need_to_deal_first_data=True, **class_name):
        """
        根据日收益率计算累计收益率
        :param data: [[yield, cumulative_rate],[yield, cumulative_rate],[yield, cumulative_rate]]
        :param rate_index: {yield.index: cumulative_rate.index}
        :param need_to_deal_first_data: if true then deal the first data special
        :return: 
        """
        # data_dealed = list(data).copy()
        data_dealed = []
        for each in data:
            data_dealed.append(list(each))
        # rate_indexes_reverse = {v: k for k, v in rate_indexes.items()}
        # 轮询需要计算收益率的数据，逐个处理
        for rate_index, cumulative_rate_index in rate_indexes.items():
            # 累计收益率初始化
            cumulative_rate = 0
            # 轮询每一条SQL 结果数据
            for num_index, info in enumerate(data):
                day_ratio = info[rate_index]
                # 是否对第一条数据特殊化处理
                if need_to_deal_first_data:
                    if "GetMonthAccountYield" in class_name.keys():
                        init_date = info[0]
                        data_dealed[num_index] = (init_date, 0, 0, None, 0)
                        need_to_deal_first_data = False
                    elif "GetMonthIndexAcYield" in class_name.keys():
                        # 轮询每条SQL结果的每个字段值，重置第一天的累计收益率为当日收益率
                        for column_index, column_value in enumerate(info):
                            if column_index in rate_indexes.keys():
                                data_dealed[num_index].pop(rate_indexes.get(column_index))
                                data_dealed[num_index].insert(rate_indexes.get(column_index), info[column_index])

                        need_to_deal_first_data = False
                else:
                    # 累计收益率计算
                    cumulative_rate = round((1 + cumulative_rate) * (1 + day_ratio) - 1, 4)
                    # 轮询每条SQL结果的每个字段值, 重置未计算的累计收益率数据
                    for column_index, column_value in enumerate(info):
                        if column_index == rate_index:
                            data_dealed[num_index].pop(rate_indexes.get(column_index))
                            data_dealed[num_index].insert(rate_indexes.get(column_index), round(cumulative_rate, 4))

        return data_dealed

def get_month_account_yield(data):
    """处理此接口的业务计算数据
    sql 结果字段顺序为： b.init_date, a.asset, b.income, (abs(b.fund_in) - abs(b.fund_out)) as net_fund_in, b.position
    columns 字段顺序为：COLUMNS = ["init_date", "income", "yield", "accumulative_yield", "position"]
    """
    data_dealed = []
    income_sum = 0
    net_fund_in_sum = 0
    max_net_fund_in_sum = 0
    for num_index, info in enumerate(data):
        data_dealed.insert(num_index, [])
        for value_index, value in enumerate(info):
            if value_index in [0]:
                data_dealed[num_index].append(value)
            elif value_index == 4:
                income_sum += info[2]
                net_fund_in_sum += info[3]
                if max_net_fund_in_sum < net_fund_in_sum:
                    max_net_fund_in_sum = net_fund_in_sum
                data_dealed[num_index].append(info[2])  # 收益
                # 若当日净流入为负， 则收益率=当日盈亏/上日总资产， 防止资金都流出的情况下， 成本很低，收益率很高的情况出现
                if info[3] > 0 :
                    data_dealed[num_index].append(round(info[2]/(info[1]+info[3]), 6))  # 收益率
                else:
                    data_dealed[num_index].append(round(info[2]/(info[1]), 6))  # 收益率
                data_dealed[num_index].append(round(income_sum/(data[0][1]+max_net_fund_in_sum), 6))  # 累计收益率
                data_dealed[num_index].append(info[4])
                
    return data_dealed
    

class HbaseResultDeal:
    """处理hbase业务数据处理, 将hbase中某字段存的list数据转化成接口响应的list格式；    
    若hbase存放是对应hbase字段名为column1, 接口响应数据对应字段为 data_list；
    则接口 deal 的list_name = [column1, data_list]"""
    def deal(self, hbase_result, is_json_content=False, list_name=["list_name_old", "list_name_new"]):
        """hbase_result is the result from hbase. 
        is_json_content: if True, then transfer data from str to list.
        list_name: the index zero is the old colunm name which is in hbase database,
            the index one is the new column name which is in the response of interface."""
        # 若已经是字典项了而不是hbase 返回的结果， 那么不做处理；
        if not isinstance(hbase_result, list):
            return hbase_result
        hbase_dict = {}
        # 判断是否要展开 json_content数据
        # if is_json_content:
        #     # json_content_list = []
        for i, each_row in enumerate(hbase_result):
            row_key = each_row.row
            for column, value in each_row.columns.items():
                hbase_dict.setdefault(column.split(":")[1], value.value)
                json_content_list = []
                # 如果字段数据是一个列表数据
                if is_json_content and column.split(":")[1] in list_name:
                    # str -->  list, '[{0},{1}]' --> [{0}, {1}]
                    data = eval(value.value.replace("[", "").replace("]", ""))
                    if isinstance(data, dict):
                        json_content_list.append(data)
                    else:
                        for each_info in data:
                            if isinstance(each_info, dict):
                                json_content_list.append(each_info)
                            else:
                                raise Exception("{0}: is not a dict. The type is {1}. ".format(each_info,
                                                                                               type(each_info)))
                    hbase_dict.setdefault(list_name[list_name.index(column.split(":")[1])+1], json_content_list)
        return hbase_dict

    def hbase_result_to_dict(self, hbase_result, func={}, **columns):
        """hbase 获取的数据 hbase data，转化成一个字典项"""
        if not hbase_result:
            return {}
        hbase_dict = {}
        for column, value in columns.items():
            # 处理hbase 获取出来的column
            if ":" in column:
                column = column.split(":")[1]
            if column != value:
                hbase_dict.setdefault(column, value)
            else:
                # 若 hbase 返回的数据为 list， 即使只有一条数据
                a = hbase_result[0]
                b = a.columns
                c = b.get(":".join(["tag_base", column]))
                if not c:
                    continue
                d = c.value
                # tmp = hbase_result[0].columns.get(column).value
                if column in func.keys():
                    hbase_dict.setdefault(column, func.get(column)(d))
                else:
                    hbase_dict.setdefault(column, d)

        return hbase_dict


class SpecialDate:
    """根据 cal_init_date， 计算近x月/年的初始日期
    cal_init_date 为当前最新的交易日，
    interval_type 为时间周期：
                        期初日期
        近一月 --> cal_init_date-30day  --> interval_type = 1
        近三月 --> cal_init_date-90day  --> interval_type = 2
        近半年 --> cal_init_date-180day  --> interval_type = 3
        近一年 --> cal_init_date-365day  --> interval_type = 4
        近两年 --> cal_init_date-730day  --> interval_type = 5
        今年 --> concat(substr(cal_init_date, from 1 for 6), '01')  --> interval_type = 9
        """
    def get_date(self, init_date, delay):
        """
        根据 init_date 往前或往后 推 delay 天， 返回处理后的日期
        :param init_date: 日期
        :param delay: 往前或往后N天
        """
        if not isinstance(init_date, str):
            init_date = str(init_date)
        if not isinstance(delay, int):
            delay = int(delay)
        return (datetime.datetime.strptime(init_date, '%Y%m%d') + datetime.timedelta(days=delay)).strftime('%Y%m%d')

    def get_init_date(self, cal_init_date, interval_type):
        """获取期初日期: 根据 传入的 interval_type值，返回不同的期初日期"""
        interval_type = int(interval_type)
        if interval_type == 1:
            init_date = self.get_date(cal_init_date, -30)
        elif interval_type == 2:
            init_date = self.get_date(cal_init_date, -90)
        elif interval_type == 3:
            init_date = self.get_date(cal_init_date, -180)
        elif interval_type == 4:
            init_date = self.get_date(cal_init_date, -365)
        elif interval_type == 5:
            init_date = self.get_date(cal_init_date, -730)
        elif interval_type == 9:
            init_date = "".join(str(cal_init_date)[:6], "01")
        return init_date

    def get_init_month(self, cal_init_date):
        """获取期初月份"""
        months = []
        init_month = self.get_date(cal_init_date, -365)[:6]
        for i in range(12):
            year = int(init_month[:4])
            month = int(init_month[-2:]) + i
            if month > 12:
                year += 1
                month -= 12
            months.append("".join([str(year), str(month).rjust(2, "0")]))
        return months

    def yield_init_date(self, start=0, end=0, step=10):
        """根据日期的期初期末时间，以及日期的增减步长，返回一个日期生成的迭代器"""
        for init_date_delay in range(start, end, step):
            init_date = (datetime.datetime.strptime(self.start_init_date, '%Y%m%d') +
                         datetime.timedelta(days=init_date_delay)).strftime('%Y%m%d')
            # print(init_date)
            yield init_date

    def month_add(self, init_date, interval=0, month_add=0):
        """同 mysql date_add 函数处理方式, 返回日期加减N个月后的日期
        init_date is a date like 20200603.
        month is a integer. Can greater than 0 or less than 0.
        """
        interval = int(interval)
        if interval == 1:
            month_add = -1
        elif interval == 2:
            month_add = -3
        elif interval == 3:
            month_add = -6
        elif interval == 4:
            month_add = -12
        year = int(init_date[:4])
        month = int(init_date[4:6])
        day = int(init_date[6:])
        month = month + month_add
        if month > 12:
            year = year + 1
            month = month - 12
        while True:
            try:
                new_date = datetime.date(year, month, day)
                return "".join([str(new_date.year), str(new_date.month), str(new_date.day)])
            except ValueError:
                day -= 1

cumulative_rate = CumulativeRate()
hbase_result_deal = HbaseResultDeal()
special_date = SpecialDate()
# print(special_date.month_add(init_date="20200430", month_add=-1))