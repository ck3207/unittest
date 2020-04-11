# -*- coding: utf-8 -*-
__author__ = "chenk"
import  json, pymysql
from log import  setup_logging
import  logging

class Connect_mysql:
    """Get Configuration and Connect to Mysql!"""
    def __init__(self):
        setup_logging()

    def get_config(self, file_name="config"):
        """Get Configuration!"""
        with open(file_name, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config

    def conn_mysql(self, host, port, user, password, database, charset="utf8"):
        """Connetct to Mysql."""
        logger = logging.getLogger(self.__class__.__name__)
        try:
            conn = pymysql.connect(host=host, port=port, user=user, password=password,  database=database, charset=charset)
            cur = conn.cursor()
            return conn, cur
        except Exception as e:
            logger.info('Connect to mysql Error!')
            logger.error(e)

    def disconnect(self, conn, cur):
        cur.close()
        conn.close()

def delete_datas(cur, conn):
    sql = "delete from asset_curve_copy where init_date = '{0}' and interval_type = {1}; \n "
    cur.execute("select date,type from temp_table")
    for each in cur.fetchall():
        execute_sql = sql.format(each[0], each[1])
        cur.execute(execute_sql)
    conn.commit()

if __name__ == "__main__":
    connect_mysql = Connect_mysql()
    mysql_config = connect_mysql.get_config("mysql_config.json")
    conn, cur = connect_mysql.conn_mysql(host=mysql_config["zhongtai_mycat_sit"]["host"], port=mysql_config["zhongtai_mycat_sit"]["port"],\
                             user=mysql_config["zhongtai_mycat_sit"]["user"], password=mysql_config["zhongtai_mycat_sit"]["password"], \
                            database=mysql_config["zhongtai_mycat_sit"]["database"], charset=mysql_config["zhongtai_mycat_sit"]["charset"])


    delete_datas(cur, conn)

    cur.close()
    conn.close()