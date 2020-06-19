import decimal

import unittest

from business.business import hbase_result_deal

class CheckResponseColumns(unittest.TestCase):
    """对于数据库中的数据与接口返回数据进行检验是否一致"""
    def checking(self, class_name, sql_result, interface_result, is_fetchone=True, **params):
        """
        :param self: unittest object
        :param class_name: a test class name
        :param sql_result: result which executed in SQL
        :param interface_result: result which returned in interface
        :param is_fetchone: whether the result get via function fetchone or not
        :param params: sql which current test executed. 
        url which current test requested. 
        data is the params in interface requesting.
        count is the num of return list.
        list_name: is a list name of response. The index zero is the old colunm name which is in hbase database,
                the index one is the new column name which is in the response of interface.
        table_columns:  is a list like ["init_date", "bond_num"]. The column in list need to be checking.
        special_column is a list like ["int_date", "bond_num"]. The column in list need to be filter when checking.
        deal_column is a list like ["bond_num", int]. The fisrt index is the name of column, 
        the second is the function which column need to be dealed specially.
        calculation is a list like {"column": ["new_column", "column1", "+", "column2"]}. 
        The first one is the new column which will returned in interface. 
        The others can be joined together as a arithmetic expressions. 
        :return: 
        """
        # SQL 执行结果为一条数据时，对比每一条记录里面的字段值
        if params.get("is_hbase_result"):
            sql_result = hbase_result_deal.deal(sql_result, is_json_content=params.get("is_json_content"),
                                                list_name=params.get("list_name"))
            # 根据传入的数据进行计算， 列表中第一个为输出的字段，其他个数为计算表达式的一部分
            if params.get("calculation"):
                expression = ""
                new_column = params.get("calculation")[0]
                for i, each in enumerate(params.get("calculation")[1:]):
                    if i % 2 == 0:
                        expression += sql_result.get(each)
                    else:
                        expression += each
                sql_result.setdefault(new_column, round(eval(expression), 4))

            # 处理接口输出字段与hbase查询出的输出字段不一致的情况
            if len(params.get("list_name", [])) > 1:
                for column in params.get("list_name"):
                    if not sql_result.get(column, False):
                        # origin_column = column
                        new_column = column
                        # list_name 允许存在多组数据， eg: [old_1, new_1, old_2, new_2]
                        if params.get("list_name").index(column) / 2 == 0:
                            tmp = params.get("list_name").index(column) + 1
                        else:
                            tmp = params.get("list_name").index(column) - 1
                        # new_column = params.get("list_name")[tmp]
                        origin_column = params.get("list_name")[tmp]
                        sql_result.setdefault(new_column, sql_result.get(origin_column))

            try:
                for column in params.get("table_columns"):
                    # 统一日志信息
                    msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
                    Hbase result is\n{4}\n column is [{5}]".format(params.get("sql"), params.get("url"), params.get("data"),
                                                                   interface_result, sql_result, column)
                    # 检验字段在 是一个list 类型 list 中的每个元素是一个dict， 那么对比list 里面每一个 dict 中的元素的数据
                    if isinstance(sql_result.get(column), list):
                        for i, info in enumerate(sql_result.get(column)):
                            if not isinstance(info, dict):
                                info = eval(info)
                            for k, v in info.items():
                                deal_column = params.get("deal_column", [False])
                                # 需要过滤的字段，即无需验证字段
                                if k in params.get("special_column", []):
                                    continue
                                # 需要验证的字段 且 不需要特殊处理的字段
                                if k not in params.get("special_column", []) and k != deal_column[0]:
                                    self.assertEqual(str(v), str(interface_result.get(column)[i].get(k)), msg=msg_model)
                                # 需要特殊处理的字段
                                elif deal_column and k == deal_column[0]:
                                    self.assertEqual(str(deal_column[1](v)), str(interface_result.get(column)[i].get(k)),
                                                     msg=msg_model)
                                # 其他
                                else:
                                    self.assertEqual(str(v), str(interface_result.get(column)[i].get(k)), msg=msg_model)
                    # 若不是过滤字段，需验证； 否则，不验证
                    elif column not in params.get("special_column", []):
                        # 若字段已被重新命名
                        # if column in params.get("list_name"):
                        #     self.assertEqual(str(sql_result.get(column)), str(interface_result.get(column)), msg=msg_model)
                        self.assertEqual(str(sql_result.get(column, '')), str(interface_result.get(column)), msg=msg_model)
                        continue

            except TypeError:
                if not locals().get(column):
                    column = "cant get."
                self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                                Current is checking column {3}".format(params.get("sql"), params.get("url"),
                                                                       params.get("data"), column))

        else:
            # 统一日志信息
            msg_model = "\nSQL is\n {0}\n Interface is\n {1}\n params is\n {2}\nInterface response is\n {3}\n\
            Hbase result is\n{4}\n ".format(params.get("sql"), params.get("url"), params.get("data"),
                                                           interface_result, sql_result)
            if is_fetchone:
                for i, column in enumerate(class_name.COLUMNS):
                    try:
                        if isinstance(sql_result[i], decimal.Decimal):
                            self.assertAlmostEqual(sql_result[i], decimal.Decimal(interface_result.get(column)), msg=msg_model)
                        else:
                            self.assertEqual(sql_result[i], interface_result.get(column), msg=msg_model)
                    except TypeError:
                        self.assertTrue(0, msg="sql is\n {0}\n interface is\n {1}\n params is {2}\n\
                        Current is checking column {3}".format(params.get("sql"), params.get("url"), params.get("data"),
                                                               column))

            else:
                # SQL 执行结果为多条数据时，需取每一条数据，对比每一条记录里面的字段值
                if params.get("count"):
                    self.assertEqual(params.get("count"), interface_result.get("count"), msg=msg_model)
                for sql_index, sql_data in enumerate(sql_result):
                    # 从 SQL 中获取的字段， 按照顺序比对每一个字段的值
                    for column_index, column in enumerate(class_name.COLUMNS):
                        try:
                            interface_data = interface_result.get("data_list")[sql_index].get(column)
                            msg = msg_model + "\nCurrent is checking column {0}\n".format(column)
                            # 该字段为时间戳， 且接口时间戳字段比 SQL 查询的时间戳字段 多了三个零，故特殊处理
                            if params.get("is_timestamp") and column in ["init_date", "part_init_date"]:
                                interface_data /= 1000
                            if isinstance(sql_data[column_index], decimal.Decimal):
                                # 累计收益率由于保留小数问题，存在一定误差
                                if params.get("is_cumulative_rate"):
                                    self.assertTrue(abs(sql_data[column_index] - decimal.Decimal(interface_data)) <= 0.0015,
                                                    msg=msg)
                                else:
                                    self.assertAlmostEqual(sql_data[column_index], round(decimal.Decimal(interface_data), 4),
                                                           msg=msg)
                            else:
                                # self.assertEqual(sql_data[column_index], interface_data, msg="sql_data:{0},interface_data:{1},\n\
                                # column_index:{2}".format(sql_data, interface_result.get("data_list")[sql_index], column_index))
                                self.assertEqual(sql_data[column_index], interface_data, msg=msg)
                        except TypeError:
                            self.assertTrue(0, msg=msg)

