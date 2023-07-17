# -*- coding: utf-8 -*-
import pymysql
from influxdb import InfluxDBClient


def get_data(ip_add, port, user_name, pass_wd, databases, measurement_name):
    db = pymysql.connect(host=ip_add, user=user_name, password=pass_wd, port=port, database=databases)
    print("Connect OK! Host info:", db.host_info, "|MySQL info:", db.get_server_info())
    cur = db.cursor()

    cur.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{measurement_name}'")

    # 提取列名数据
    column_names = [column[0] for column in cur]
    print(column_names)
    # 打印列名
    for column_name in column_names:
        print(column_name)
    # 提取数据
    sql = 'select * from %s limit 10;' % measurement_name
    cur.execute(sql)
    result = cur.fetchall()  # 显示全部数据
    print(result)
    print()
    db.close()
    # print(result)
    return result  # 函数返回值


if __name__ == "__main__":
    ipaddress = input("ipaddress:")
    port_in = input("Mysql_port:")
    username = input("username:")
    password = input("password:")
    database = input("database_name:")
    maen = input("measurement_name:")
    # ipaddress = "your MySQL ip"
    # port_in = 3306  # mysql 端口，默认3306
    # username = "your MySQL username"
    # password = "your MySQL password"
    # database = "your MySQL database name"
    # maen = "MySQL数据表名称"
    get_data(ipaddress, int(port_in), username, password, database, maen)
