import os
import re
import sys
import pickle

class DealDataFile:
    def __init__(self, filename):
        self.filename = filename
        self.file = ""
        self.tables_info = {}
        self.sqls_info = {}

    def _open_file(self):
        if os.path.exists(self.filename):
            self.file = open(file=self.filename, mode="r", encoding="utf-8")

        return self.file

    def _close_file(self):
        self.file.close()

    def get_tables_info(self):
        table_flag = False
        self.tables_info = {}    # {table1: [col1, col2, col3], table2: [col1, col2, col3]}
        while True:
            line = self.file.readline()
            if line == "":
                break
            elif line.strip().endswith(")"):
                table_flag = False
                continue
            elif line.strip().startswith("--"):
                continue
            elif line.strip() == "":
                continue
            else:
                line = line.replace("(\n", "")
            if line.strip().startswith("create external table"):
                table_name = re.findall(pattern="([\w]+)\s*", string=line.strip())[3]
                if table_name not in self.tables_info.keys():
                    self.tables_info.setdefault(table_name, [])
                    table_flag = True
                    continue

            if table_flag:
                column_info = self.get_column_info(line)
                self.tables_info.get(table_name).append(column_info)

        return self.tables_info

    def get_column_info(self, line=""):
        re_result = re.findall(pattern="([\S]+)\s+?", string=line)
        if len(re_result) >= 2:
            return re_result[:2]
        else:
            return []

    def stitching_sql(self, table_name):
        sql = ""
        for i, each in enumerate(self.tables_info.get(table_name)):
            try:
                if each[1].count("decimal") >= 1 or each[1].count("int") >= 1:
                    sql += "".join(["{", str(i), "}", ","])
                elif each[1].count("string") >= 1:
                    sql += "".join(['''"{''', str(i), '''}"''', ","])
                else:
                    print("Error: ", table_name, each)
                    sys.exit(1)
            except Exception as e:
                print(str(e))
                print("Error:", table_name, each)
        self.sqls_info.setdefault(table_name, sql[:-1])
        return sql[:-1]

    def get_sqls_info(self):
        return self.sqls_info


deal_data_file = DealDataFile("../files/hbase_create_2.sql")
deal_data_file._open_file()
for table_name, columns in deal_data_file.get_tables_info().items():
    sql = deal_data_file.stitching_sql(table_name=table_name)
    # print(table_name)
    # print(sql)
sqls_info = deal_data_file.get_sqls_info()


