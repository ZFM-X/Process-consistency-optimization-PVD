#!/usr/bin/python3.8.4 (python版本)
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2024/1/9 11:54
# @File    : fz_R2R.py
import time
from opcua import Client
import numpy as np
from caculate import para_init, fz_R2R
from data_process import qury_CK, CK_to_database, fz_mh_to_database, fz_to_database
import pandas as pd
import pymysql


# ************************************************ 数据库获取数据 ****************************************************** #
# ############################################### 获取单个位置方阻 ##################################################### #
def query_fz(machine_id, tube, key_boat, begin_pos, end_pos, fz_T):
    """
    连接 MySQL 数据库，执行查询，并将查询结果转化为 Pandas DataFrame 对象
    :return: Pandas DataFrame 对象
    """
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', db="ods_fz")
    cursor = conn.cursor()

    sql_query = f'''SELECT event_time, tube_id, boat_id, zone_id, slice_value, average FROM fz_y_{machine_id}_{tube}'''
    cursor.execute(sql_query)

    # 获取查询结果,转化为 Pandas DataFrame 对象
    result = cursor.fetchall()
    batch_fz = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    batch_fz['slice_value'] = batch_fz['slice_value'].astype(int)
    batch_fz['average'] = batch_fz['average'].astype(float)

    if batch_fz[(batch_fz['slice_value'] >= begin_pos) & (batch_fz['slice_value'] <= end_pos) & (
            batch_fz['zone_id'] == key_boat) & (batch_fz['tube_id'] == tube)]['average'].mean() is np.nan:
        fz = fz_T
    else:
        fz = batch_fz[(batch_fz['slice_value'] >= begin_pos) & (batch_fz['slice_value'] <= end_pos) & (
                batch_fz['zone_id'] == key_boat) & (batch_fz['tube_id'] == tube)]['average'].mean()

    cursor.close()
    conn.close()
    return fz


# ################################################ 清空方阻数据库 ###################################################### #
def clear_mysql_batch_data(machine_id, tube):
    """
    定义函数清空单批次数据
    """
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', db="ods_fz")
    cursor = conn.cursor()

    sql_query = f'''TRUNCATE TABLE fz_y_{machine_id}_{tube}'''
    cursor.execute(sql_query)

    cursor.close()
    conn.close()


# ************************************************ 配置表获取数据 ****************************************************** #
if __name__ == '__main__':
    # 初始字典，用于判断机台是否需要初始化
    run_once = {}
    # 创建字典，用于人为调整后初始化
    DLV_in_out = {}

    # 使用循环监
    while True:
        ################################################################################################################
        try:
            # 查询数据库，获取机台信息
            conn = pymysql.connect(host="10.254.0.82", port=3306, user="root", password="000000", db="ods_base",
                                   charset="utf8")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM config_machine WHERE machine_id LIKE '6_%'")
            result = cursor.fetchall()
            config_machine = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
            cursor.close()
            conn.close()
        except:
            print('机台设置数据库连接失败！！！')
            continue

        # 获取机台号，定义管号列表
        machine_list = config_machine['machine_id'].tolist()
        tube_list = ['1', '2', '3', '4', '5', '6']
        ################################################################################################################
        # 根据机台号，获取机台配置信息
        for machine_id in machine_list:
            # 确保 run_once 字典中有 machine 这个键，并且它的值是一个字典
            if machine_id not in run_once:
                run_once[machine_id] = {}
                DLV_in_out[machine_id] = {}

            # 获取机台当前状态和调整策略和机台opc地址
            status = config_machine[config_machine['machine_id'] == machine_id]['status'].values
            strategy = config_machine[config_machine['machine_id'] == machine_id]['strategy'].values
            opc_address = config_machine[config_machine['machine_id'] == machine_id]['opc_address'].values

            # 根据机台当前状态和调整策略，查询配置参数数据库，获取相应参数
            conn = pymysql.connect(host="10.254.0.82", port=3306, user="root", password="000000", db="ods_base",
                                   charset="utf8")
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM config_parameter WHERE strategy = '{strategy[0]}' AND process = 'pdf'")
            result = cursor.fetchall()
            config_parameter = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
            cursor.close()
            conn.close()
            ############################################################################################################
            # 将需要的配置信息转化为列表
            loop_name = config_parameter['loop_name'].tolist()
            key_boat = config_parameter['key_boat'].tolist()
            opc_tag = config_parameter['opc_tag'].tolist()
            begin_pos = config_parameter['begin_pos'].tolist()
            end_pos = config_parameter['end_pos'].tolist()
            fz_T = config_parameter['target'].tolist()
            y_low = config_parameter['y_low'].tolist()
            y_up = config_parameter['y_up'].tolist()
            warn_low = config_parameter['warn_low'].tolist()
            warn_up = config_parameter['warn_low'].tolist()
            x_low = config_parameter['x_low'].tolist()
            x_up = config_parameter['x_up'].tolist()
            x_max = config_parameter['x_max'].tolist()
            A_r = config_parameter['gain'].tolist()
            W = config_parameter['w'].tolist()
            pianshu_LL = config_parameter['pianshu_LL'].tolist()
            mh_T = config_parameter['mh_T'].tolist()
            A_mh = config_parameter['A_mh'].tolist()

            begin_pos = [float(x) for x in begin_pos]
            end_pos = [float(x) for x in end_pos]
            fz_T = [float(x) for x in fz_T]
            y_low = [float(x) for x in y_low]
            y_up = [float(x) for x in y_up]
            warn_low = [float(x) for x in warn_low]
            warn_up = [float(x) for x in warn_up]
            x_low = [float(x) for x in x_low]
            x_up = [float(x) for x in x_up]
            x_max = [float(x) for x in x_max]
            A_r = [float(x) for x in A_r]
            W = [float(x) for x in W]
            pianshu_LL = [float(x) for x in pianshu_LL]
            mh_T = [float(x) for x in mh_T]
            A_mh = [float(x) for x in A_mh]

            # 根据loop_name新建列表，用于存储数据时的列名
            CK_name = [item + '_CK' for item in loop_name]
            DLV_out_name = [item + '_DLV_out' for item in loop_name]
            fz_name = [item + '_fz' for item in loop_name]
            DLV_in_name = [item + '_DLV_in' for item in loop_name]
            ############################################################################################################
            # 根据管的数据库状态，判断是否计算
            for tube in tube_list:
                # 监控方阻数据库变化
                try:
                    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', db="ods_fz")
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT MAX(event_time) FROM fz_y_{machine_id}_{tube}")
                    result = cursor.fetchall()
                    result = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
                    cursor.close()
                    conn.close()
                except:
                    print(f'{machine_id}号机台{tube}数据库不存在！！！')
                    continue

                DLV_in_list = []
                fz_list = []
                CK_1_list = []
                CK_list = []
                DLV_out_list = []

                # 数据库数据最新时间和当前时间差距是否有半小时，作为触发条件
                if result['MAX(event_time)'][0] is not None:
                    event_time = result['MAX(event_time)'][0]
                    diff_time = pd.Timestamp.now() - event_time
                    diff_time = pd.to_timedelta(diff_time)
                    diff_time_miaoshu = diff_time.seconds
                    print(f'{machine_id}号机台，{tube}号管，差距秒数：{diff_time_miaoshu}')
                    ####################################################################################################
                    try:
                        # 循环读取读取当前机台，当前管输入：列表
                        client = Client(opc_address[0])
                        client.connect()
                        for j in range(len(loop_name)):
                            myvar = client.get_node('ns=2;s=Tag.Tube' + str(tube) + str(opc_tag[j]))
                            DLV_in = myvar.get_value()
                            DLV_in_list.append(DLV_in)
                        print(f'输入{DLV_in_list}')
                        client.disconnect()
                    except:
                        print(f'{machine_id}号机台{tube}号管输入获取失败！！！')
                        continue
                    ####################################################################################################
                    # 第一次计算，初始化CK，不更新CK
                    if diff_time_miaoshu >= 1800 and status[0] == '1' and (f'Tube_{tube}' not in run_once[machine_id] or DLV_in_out[f'{machine_id}'][f'Tube_{tube}'] != DLV_in_list):
                        # 初始化参数变为1
                        run_once[f'{machine_id}'][f'Tube_{tube}'] = 1

                        # 循环计算各温区
                        for i in range(len(loop_name)):
                            # 参数初始化+正常计算
                            CK_list.insert(i, para_init(fz_T[i], mh_T[i], DLV_in_list[i], A_r[i], A_mh[i]))
                            fz_list.insert(i, query_fz(machine_id, tube, key_boat[i], begin_pos[i], end_pos[i], fz_T[i]))

                        print('R2R-Initialize')
                        print(fz_list)
                        print(CK_list)

                        R2R_status = 'Initialize'

                        # 保存数据
                        CK_to_database(machine_id, tube, CK_name, CK_list)
                        fz_to_database(machine_id, tube, fz_name, fz_list)
                        fz_mh_to_database(machine_id, tube, fz_name, fz_list, CK_name, CK_list, DLV_in_name,
                                          DLV_in_list, DLV_out_name, DLV_out_list, R2R_status)

                        # 清空数据库
                        clear_mysql_batch_data(machine_id, tube)
                    ####################################################################################################
                    # 正常计算
                    elif diff_time_miaoshu >= 1800 and status[0] == '1' and run_once[f'{machine_id}'][f'Tube_{tube}'] == 1:
                        # 循环计算各温区
                        for i in range(len(loop_name)):
                            fz_list.insert(i, query_fz(machine_id, tube, key_boat[i], begin_pos[i], end_pos[i], fz_T[i]))
                            # 数据读取CK_1
                            CK_1_list = qury_CK(machine_id, tube)
                            CK, DLV_out = fz_R2R(fz_T[i], fz_list[i], mh_T[i], DLV_in_list[i], CK_1_list[i], W[i],
                                                 A_r[i], A_mh[i])
                            CK_list.insert(i, CK)
                            DLV_out_list.insert(i, DLV_out)

                        # 字典记录计算输出，用于对比
                        DLV_in_out[f'{machine_id}'][f'Tube_{tube}'] = DLV_out_list

                        print('R2R-Execute')
                        print(fz_list)
                        print(CK_list)

                        R2R_status = 'Execute'

                        # 保存数据
                        CK_to_database(machine_id, tube, CK_name, CK_list)
                        fz_to_database(machine_id, tube, fz_name, fz_list)
                        fz_mh_to_database(machine_id, tube, fz_name, fz_list, CK_name, CK_list, DLV_in_name,
                                          DLV_in_list, DLV_out_name, DLV_out_list, R2R_status)

                        # 清空数据库
                        clear_mysql_batch_data(machine_id, tube)
            print(run_once)
            print(f'{machine_id}号机，完成！！！')
        time.sleep(5)
