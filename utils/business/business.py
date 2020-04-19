

class CumulativeRate:

    def cal_cumulative_rate(self, data=[], rate_indexes={0: 4}, need_to_deal_first_data=True, **class_name):
        """
        
        :param data: [[],[],[]]
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


cumulative_rate = CumulativeRate()