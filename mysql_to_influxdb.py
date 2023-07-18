# -*- coding: utf-8 -*-
import pymysql
import json
import datetime
from jsonpath import jsonpath as jsp
from influxdb import InfluxDBClient


def get_data(ip_add, port, user_name, pass_wd, databases, measurement_name):
    try:
        db = pymysql.connect(host=ip_add, user=user_name, password=pass_wd, port=port, database=databases)
        print("Connect OK! Host info:", db.host_info, "|MySQL info:", db.get_server_info())
        cur = db.cursor()
        # 查询列名
        cur.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{measurement_name}'")

        # 提取列名数据
        column_names = [column[0] for column in cur]
        # print(column_names)

        # 提取数据
        sql = 'select * from %s ;' % measurement_name
        cur.execute(sql)
        result = cur.fetchall()  # 显示全部数据
        # print(result)
        return column_names, result  # 函数返回值

    except Exception as er:
        print(er)

    finally:
        db.close()


def process_data(column_names, result):
    # 打印列名
    # print(column_names)
    formatted_dict = {str(index): value for index, value in enumerate(column_names)}
    print("'索引':'列名'：", formatted_dict)

    # for column_name in column_names:
    #     print(column_name)

    # 转换格式函数
    def format_datetime(dt):
        return dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(dt, datetime.datetime) else dt

    # 循环处理数据
    formatted_data_list = []
    for item in result:
        formatted_item = tuple(format_datetime(el) for el in item)
        formatted_data_list.append(formatted_item)

    # 输出结果
    for item in formatted_data_list:
        print(item)


if __name__ == "__main__":
    print("请选择模式：【1】读取配置文件||【2】手都输入连接信息")

    mode = input("请选择模式【1】OR【2】:")
    if mode == "1":
        # 读取 配置 文件
        with open('config.json', 'r') as file:
            # 读取 JSON 数据并解析
            data = json.load(file)
            # print(data)
        ipaddress = jsp(data, "$.mysql_connect.ipaddress")[0]
        port_in = jsp(data, "$.mysql_connect.port_in")[0]
        username = jsp(data, "$.mysql_connect.username")[0]
        password = jsp(data, "$.mysql_connect.password")[0]
        database = jsp(data, "$.mysql_connect.database")[0]
        mean = jsp(data, "$.mysql_connect.mean")[0]
        # print(ipaddress, port_in, username, password, database, mean)
        column_names, result = get_data(ipaddress, int(port_in), username, password, database, mean)
        process_data(column_names, result)

    elif mode == "2":
        ipaddress = input("ipaddress:")
        port_in = input("Mysql_port:")
        username = input("username:")
        password = input("password:")
        database = input("database_name:")
        mean = input("measurement_name:")
        # ipaddress = "your MySQL ip"
        # port_in = 3306  # mysql 端口，默认3306
        # username = "your MySQL username"
        # password = "your MySQL password"
        # database = "your MySQL database name"
        # mean = "MySQL数据表名称"
        column_names, result = get_data(ipaddress, int(port_in), username, password, database, mean)
        process_data(column_names, result)