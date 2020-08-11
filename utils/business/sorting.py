import random


class Sorting:
    def __init__(self):
        pass

    def sorting_data_list(self, data_list, sorting_columns=[], is_reverse=False):
        """
        :param data_list: 数据列表, 例如：[{"column1": value1, "column2": value2}] 
        :param sorting_columns:  排序字段， 根据字段先后关系，定义优先级； 
        例如： ["hold_status", "income"], 一级排序为：hold_status， 二级排序为， income
        :param is_reverse: 默认为升序，设置为 True时，为降序；
        :return: 
        """
        list_dealed = []
        list_final = []
        if isinstance(data_list, list):
            for i, data in enumerate(data_list):
                tmp = []
                for sorting_column in sorting_columns:
                    tmp.append(data.get(sorting_column))
                tmp.insert(0, i)
                list_dealed.append(tmp)
        list_dealed = sorted(list_dealed, key=lambda l: (l[1], l[2]), reverse=is_reverse)
        for each in list_dealed:
            list_final.append(data_list[each[0]])
        return list_final

    def sorting_data_list_2(self, data_list, sorting_rule=[1, "asc", 2, "desc"]):
        """
        :param data_list: 数据列表, 例如：[{"column1": value1, "column2": value2}] 
        :param sorting_rule:  排序字段， 根据字段先后关系，定义优先级； 
        例如： ["hold_status", "income"], 一级排序为：hold_status， 二级排序为， income
        :param is_reverse: 默认为升序，设置为 True时，为降序；
        :return: 
        """
        test_list = []
        for i in range(10):
            for j in range(10):
                test_list.append((i*10+j, random.randint(0, 100), random.randint(0, 100)))
        print(test_list)
        data_list = test_list

    def __sort(self, data_list, sorting_rule):
        if isinstance(data_list, list):
            if len(data_list) == 1:
                return data_list
            elif len(data_list) == 2:
                if isinstance(data_list[0], list):
                    # 第一个排序字段的规则的字段索引：升序/降序(asc/desc)
                    rule_index = 1
                    while True:
                        # 若排序规则为：升序
                        if sorting_rule[rule_index] == "asc":
                            # 若一级排序，能直接能比较出大小，则直接返回排序结果
                            if data_list[0][sorting_rule[rule_index-1]] > data_list[1][sorting_rule[rule_index-1]]:
                                return [data_list[1], data_list[0]]
                            elif data_list[0][sorting_rule[rule_index-1]] < data_list[1][sorting_rule[rule_index-1]]:
                                return [data_list[0], data_list[1]]
                            # 若一级排序对应数值相等，需进行二级排序；二级排序仍然相等，则需要三级排序...
                            else:
                                rule_index += 2
                        elif sorting_rule[rule_index] == "desc":
                            # 若一级排序，能直接能比较出大小，则直接返回排序结果
                            if data_list[0][sorting_rule[rule_index-1]] > data_list[1][sorting_rule[rule_index-1]]:
                                return [data_list[0], data_list[1]]
                            elif data_list[0][sorting_rule[rule_index-1]] < data_list[1][sorting_rule[rule_index-1]]:
                                return [data_list[1], data_list[0]]
                            # 若一级排序对应数值相等，需进行二级排序；二级排序仍然相等，则需要三级排序...
                            else:
                                rule_index += 2
                        if rule_index >= len(data_list[0]) - 1:
                            return data_list
            else:
                return self.__sort(data_list, sorting_rule)


a = [{'stock_code': '002460', 'stock_name': '赣锋锂业', 'stock_type': '1', 'exchange_type': '2', 'income': 18453.71, 'hold_day': 20.4, 'amount': 36.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '600388', 'stock_name': '龙净环保', 'stock_type': '0', 'exchange_type': '1', 'income': 56369.04, 'hold_day': 25.15, 'amount': 69.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '601318', 'stock_name': '中国平安', 'stock_type': '1', 'exchange_type': '1', 'income': 15771.39, 'hold_day': 86.87, 'amount': 87.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '002142', 'stock_name': '宁波银行', 'stock_type': '1', 'exchange_type': '2', 'income': 5850.04, 'hold_day': 51.75, 'amount': 29.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '300498', 'stock_name': '温氏股份', 'stock_type': '0', 'exchange_type': '2', 'income': 63798.67, 'hold_day': 19.75, 'amount': 76.0, 'hold_status': '0', 'money_type': '0'}, {'stock_code': '002415', 'stock_name': '海康威视', 'stock_type': '0', 'exchange_type': '2', 'income': 28721.08, 'hold_day': 86.07, 'amount': 98.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '002475', 'stock_name': '立讯精密', 'stock_type': '1', 'exchange_type': '2', 'income': 10794.87, 'hold_day': 70.34, 'amount': 87.0, 'hold_status': '0', 'money_type': '0'}, {'stock_code': '601628', 'stock_name': '中国人寿', 'stock_type': '0', 'exchange_type': '1', 'income': 84726.18, 'hold_day': 2.73, 'amount': 40.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '601328', 'stock_name': '交通银行', 'stock_type': '0', 'exchange_type': '1', 'income': 85959.36, 'hold_day': 24.69, 'amount': 91.0, 'hold_status': '1', 'money_type': '0'}, {'stock_code': '601668', 'stock_name': '中国建筑', 'stock_type': '1', 'exchange_type': '1', 'income': 103241.48, 'hold_day': 47.23, 'amount': 81.0, 'hold_status': '0', 'money_type': '0'}, {'stock_code': '603288', 'stock_name': '海天味业', 'stock_type': '1', 'exchange_type': '1', 'income': 38668.79, 'hold_day': 40.99, 'amount': 23.0, 'hold_status': '0', 'money_type': '0'}, {'stock_code': '300033', 'stock_name': '同花顺', 'stock_type': '1', 'exchange_type': '2', 'income': 216.11, 'hold_day': 99.76, 'amount': 98.0, 'hold_status': '0', 'money_type': '0'}]
sorting = Sorting()
if __name__ == "__main__":
    # sorting.sorting_data_list(data_list=a, sorting_columns=["hold_status", "income"])
    data_list = sorting.sorting_data_list_2(data_list=a, sorting_rule=[1, "asc", 2, "desc"])
    print(data_list)

