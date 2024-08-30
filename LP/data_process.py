#!/usr/bin/python3.8.4 (python版本)
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2024/1/9 11:12
# @File    : data_process.py
import pymysql
import pandas as pd
from sqlalchemy import create_engine


# ##################################################### 读取CK ######################################################## #
def qury_CK(machine_id, tube):
    """
    获取CK
    :return: 返回CK列表，包含时间，在最后
    """
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', database='5#-6#')
    cursor = conn.cursor()

    sql_query = f"""SELECT * FROM CK_history_{machine_id}_{tube} ORDER BY jointime DESC LIMIT 1"""
    cursor.execute(sql_query)

    result = cursor.fetchall()
    CK = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    CK_list = CK.iloc[0].tolist()
    CK_list.pop()

    cursor.close()
    conn.close()

    return CK_list


# ################################################### 读取调整下限 ###################################################### #
def qury_DLV_in_LL(machine_id, tube):
    """
    获取CK
    :return: 返回CK列表，包含时间，在最后
    """
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', database='5#-6#')
    cursor = conn.cursor()

    sql_query = f"""SELECT * FROM dlv_ll_{machine_id}_{tube} ORDER BY jointime DESC LIMIT 1"""
    cursor.execute(sql_query)

    result = cursor.fetchall()
    DLV_LL = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    DLV_LL = DLV_LL.iloc[0].tolist()
    DLV_LL.pop()

    cursor.close()
    conn.close()

    return DLV_LL


# ##################################################### 读取方阻 ######################################################## #
def qury_fz(machine_id, tube):
    """
    获取CK
    :return: 返回CK列表，包含时间，在最后
    """
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', database='5#-6#')
    cursor = conn.cursor()

    sql_query = f"""SELECT * FROM fz_history_{machine_id}_{tube} ORDER BY jointime DESC LIMIT 1"""
    cursor.execute(sql_query)

    result = cursor.fetchall()
    fz = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    fz_list = fz.iloc[0].tolist()
    fz_list.pop()

    cursor.close()
    conn.close()

    return fz_list


# ************************************************* 定义函数保存各参数 ************************************************** #
# ##################################################### 保存CK ######################################################## #
def CK_to_database(machine_id, tube, CK_name, CK_list):
    """
    CK保存
    :param machine_id: 机台号
    :param tube: 管号
    :param CK_name: CK列名
    :param CK_list: CK值
    :return:
    """
    # 建立到MySQL数据库的引擎
    engine = create_engine('mysql+pymysql://root:000000@10.254.0.82/5#-6#')

    # 列名处理
    time_column_name = 'jointime'
    name = CK_name + [time_column_name]

    # 数据处理
    time = str(pd.Timestamp.now())
    value = CK_list + [time]

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'CK_history_{machine_id}_{tube}', engine, if_exists='append', index=False)


# ################################################## 保存输出下限 ###################################################### #
def DLV_LL_to_database(machine_id, tube, DLV_LL_name, DLV_LL_list):
    """
    CK保存
    :param machine_id: 机台号
    :param tube: 管号
    :param CK_name: CK列名
    :param CK_list: CK值
    :return:
    """
    # 建立到MySQL数据库的引擎
    engine = create_engine('mysql+pymysql://root:000000@10.254.0.82/5#-6#')

    # 列名处理
    time_column_name = 'jointime'
    name = DLV_LL_name + [time_column_name]

    # 数据处理
    time = str(pd.Timestamp.now())
    value = DLV_LL_list + [time]

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'dlv_ll_{machine_id}_{tube}', engine, if_exists='replace', index=False)


# ################################################### 方阻保存 ######################################################## #
def fz_to_database(machine_id, tube, fz_name, fz_list):
    """
    CK保存
    :param machine_id: 机台号
    :param tube: 管号
    :param fz_name: CK列名
    :param fz_list: CK值
    :return:
    """
    # 建立到MySQL数据库的引擎
    engine = create_engine('mysql+pymysql://root:000000@10.254.0.82/5#-6#')

    # 列名处理
    time_column_name = 'jointime'
    name = fz_name + [time_column_name]

    # 数据处理
    time = str(pd.Timestamp.now())
    value = fz_list + [time]

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'fz_history_{machine_id}_{tube}', engine, if_exists='append', index=False)


# ################################################# R2R参数保存 ####################################################### #
def fz_mh_to_database(machine_id, tube, fz_name, fz_list, CK_name, CK_list, DLV_in_name, DLV_in_list, DLV_out_name,
                      DLV_out_list, status):
    """
    R2R参数保存
    :param machine_id: 机台号
    :param tube: 管号
    :param fz_name: 方阻列名
    :param fz_list: 方阻
    :param CK_name: CK列名
    :param CK_list: Ck值
    :param DLV_in_name: 输入列名
    :param DLV_in_list: 输入值
    :param DLV_out_name:输出列名
    :param DLV_out_list: 输出值
    :return:
    """
    # 建立到MySQL数据库的引擎
    engine = create_engine('mysql+pymysql://root:000000@10.254.0.82/5#-6#')

    # 设计列名
    time_column_name = 'time'
    tube_name = 'tube'
    R2R_status = 'status'
    name = fz_name + CK_name + DLV_in_name + DLV_out_name
    name.append(tube_name)
    name.append(time_column_name)
    name.append(R2R_status)

    # 设计值
    time = str(pd.Timestamp.now())
    tube_num = tube
    value = fz_list + CK_list + DLV_in_list + DLV_out_list
    value.append(tube_num)
    value.append(time)
    value.append(status)

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'fz_mh_history_{machine_id}', engine, if_exists='append', index=False)


# ################################################## 前馈过程参数保存 ################################################## #
def mh_fz_to_database(machine_id, tube, mh_name, mh_list, fz_name, fz_list, CK_name, CK_list, DLV_in_name, DLV_in_list,
                      DLV_out_name, DLV_out_list, mh_count_name, mh_count, fz_pred_name, fz_pred, status):
    """
    前馈参数保存
    :param machine_id: 机台号
    :param tube: 管号
    :param mh_name: 膜厚列名
    :param mh_list: 膜厚
    :param fz_name: 方阻列名
    :param fz_list: 方阻
    :param CK_name: CK列名
    :param CK_list: Ck值
    :param DLV_in_name: 输入列名
    :param DLV_in_list: 输入值
    :param DLV_out_name:输出列名
    :param DLV_out_list: 输出值
    :param mh_count_name: 膜厚片数
    :param mh_count: 膜厚片数
    :param fz_pred_name: 预测方阻列名
    :param fz_pred: 预测方阻
    :return:
    """
    # 建立到MySQL数据库的引擎
    engine = create_engine('mysql+pymysql://root:000000@10.254.0.82/5#-6#')

    # 设计列名
    time_column_name = 'time'
    tube_name = 'tube'
    Feedforward_status = 'status'
    name = mh_name + fz_name + CK_name + DLV_in_name + DLV_out_name + mh_count_name + fz_pred_name
    name.append(tube_name)
    name.append(time_column_name)
    name.append(Feedforward_status)

    # 设计值
    time = str(pd.Timestamp.now())
    tube_num = tube
    value = mh_list + fz_list + CK_list + DLV_in_list + DLV_out_list + mh_count + fz_pred
    value.append(tube_num)
    value.append(time)
    value.append(status)

    df = pd.DataFrame([value], columns=name)
    df.to_sql(f'mh_fz_history_{machine_id}', engine, if_exists='append', index=False)
