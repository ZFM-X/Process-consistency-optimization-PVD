#!/usr/bin/python3.8.4 (python版本)
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2024/1/9 11:55
# @File    : feedforward.py
import time
import numpy as np
from caculate import para_init, mh_feedforward
import pandas as pd
import pymysql
from opcua import Client
from data_process import qury_CK, mh_fz_to_database, qury_fz, CK_to_database, DLV_LL_to_database, qury_DLV_in_LL


# ############################################ 定义上料和检测片位匹配函数 ################################################ #
def wafer_to_slice(value):
    """
    定义函数将上下料片位匹配
    """
    if 1 <= value <= 50:
        add = 100 - (2 * value) + 2
    elif 51 <= value <= 100:
        add = 100 - 2 * (100 - value)
    elif 101 <= value <= 150:
        add = 100 - 2 * (100 - (201 - value)) - 1
    else:
        add = 100 - (2 * (201 - value) - 1)

    return add


# ************************************************** 数据获取 ********************************************************* #
# ######################################### 将膜厚数据读取到变量中，加快获取速度 ########################################## #
def qury_mohou() -> pd.DataFrame:
    """
    获取所有膜厚
    :return:
    """
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', db="ods_base")
    cursor = conn.cursor()

    sql_query = """SELECT `ModuleResults.Thickness-Layerfit.Th (L5)`, newWaferId FROM lp2_total_twoday"""
    cursor.execute(sql_query)
    result = cursor.fetchall()
    mohou_data = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    mohou_data.rename(columns={'newWaferId': 'wafer_id'}, inplace=True)

    cursor.close()
    conn.close()

    return mohou_data


# ######################################### 将膜厚数据读取到变量中，加快获取速度 ########################################## #
def Aggregate_mh(machine_id, tube, mh_T, boat_id, begin_pos, end_pos, key_boat, mohou_data):
    """
    聚合膜厚和片数
    :param machine_id: 机台号
    :param tube: 管号
    :param boat_id: 舟号
    :param begin_pos: 开始片位
    :param end_pos: 结束片位
    :param key_boat: 关键小舟号
    :return:
    """
    machine_id = machine_id.replace("_", "#")
    # ####################################### 根据机台号和舟号获取wafer ################################################# #
    conn = pymysql.connect(host='10.254.0.82', user='root', password='000000', db="ods_base")
    cursor = conn.cursor()

    sql_query = f"""SELECT wafer_id, in_time, boat_id, solt_code, wafer_add FROM wafer_boatload_data WHERE 
    machine_code = '{machine_id}' AND boat_id = '{boat_id}' and in_time >= DATE_SUB(NOW(), INTERVAL 3 HOUR)"""
    cursor.execute(sql_query)
    result = cursor.fetchall()
    wafer_id = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    cursor.close()
    conn.close()

    # ############################################## 膜厚和wafer匹配 ################################################## #
    wafer = pd.merge(wafer_id, mohou_data, on='wafer_id', how='inner')
    wafer.rename(columns={'ModuleResults.Thickness-Layerfit.Th (L5)': 'mh'}, inplace=True)

    # 匹配上料，下料片位
    wafer['wafer_add'] = wafer['wafer_add'].astype(int)
    wafer['WaferPos'] = wafer['wafer_add'].apply(lambda x: wafer_to_slice(x))
    wafer['WaferPos'] = wafer['WaferPos'].astype(float)
    wafer['mh'] = wafer['mh'].astype(float)

    # ################################################## 膜厚聚合 ##################################################### #
    if wafer[(wafer['WaferPos'] >= begin_pos) & (wafer['WaferPos'] <= end_pos) & (wafer['solt_code'] == key_boat)][
        'mh'].mean() is np.nan:
        mh = mh_T
    else:
        mh = \
            wafer[(wafer['WaferPos'] >= begin_pos) & (wafer['WaferPos'] <= end_pos) & (wafer['solt_code'] == key_boat)][
                'mh'].mean()

    # ################################################ 膜厚片数计算 #################################################### #
    mh_count = \
        wafer[
            (wafer['WaferPos'] >= begin_pos) & (wafer['WaferPos'] <= end_pos) & (wafer['solt_code'] == key_boat)].shape[
            0]

    return mh, mh_count


########################################################################################################################
if __name__ == '__main__':
    # 初始字典，用于判断机台是否需要初始化，以及防止重复进舟
    run_once = {}
    trigger = {}
    DLV_in_out = {}

    while True:
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

        # 获取机台号，设置管号
        machine_list = config_machine['machine_id'].tolist()
        tube_list = ['1', '2', '3', '4', '5', '6']
        ################################################################################################################
        # 根据机台号获取相信配置信息
        for machine_id in machine_list:
            # 确保 run_once 字典中有 machine 这个键，并且它的值是一个字典
            if machine_id not in run_once:
                run_once[machine_id] = {}
                trigger[machine_id] = {}
                DLV_in_out[machine_id] = {}

            # 获取机台当前状态和调整策略
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
            # 将有用配置信息变为列表
            loop_name = config_parameter['loop_name'].tolist()
            key_boat = config_parameter['key_boat'].tolist()
            begin_pos = config_parameter['begin_pos'].tolist()
            end_pos = config_parameter['end_pos'].tolist()
            fz_T = config_parameter['target'].tolist()
            y_up = config_parameter['y_up'].tolist()
            y_low = config_parameter['y_low'].tolist()
            warn_low = config_parameter['warn_low'].tolist()
            warn_up = config_parameter['warn_low'].tolist()
            x_up = config_parameter['x_up'].tolist()
            x_max = config_parameter['x_max'].tolist()
            A_r = config_parameter['gain'].tolist()
            W = config_parameter['w'].tolist()
            pianshu_LL = config_parameter['pianshu_LL'].tolist()
            opc_tag = config_parameter['opc_tag'].tolist()
            mh_T = config_parameter['mh_T'].tolist()
            A_mh = config_parameter['A_mh'].tolist()

            begin_pos = [float(x) for x in begin_pos]
            end_pos = [float(x) for x in end_pos]
            fz_T = [float(x) for x in fz_T]
            y_up = [float(x) for x in y_up]
            y_low = [float(x) for x in y_low]
            warn_low = [float(x) for x in warn_low]
            warn_up = [float(x) for x in warn_up]
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
            mh_name = [item + '_mh' for item in loop_name]
            mh_count_name = [item + '_count' for item in loop_name]
            fz_pred_name = [item + '_fz_pred' for item in loop_name]
            DLV_out_LL = [item + '_dlv_LL' for item in loop_name]
            ############################################################################################################
            try:
                # 连接机台，获取机台当前状态
                client = Client(opc_address[0])
                client.connect()
            except:
                print(f'{machine_id}号机台连接失败！！！')
                continue

            for tube in tube_list:
                try:
                    # 获取当前工艺步名称
                    name = client.get_node(f'ns=2;s=Tag.Tube{tube}Msg.Other.CurrentRecipeStepName')
                    process_step = name.get_value()
                except:
                    print(f'{machine_id}机台{tube}号管状态获取失败！！！')
                    continue

                print(process_step)

                mh_list = []
                mh_count_list = []
                DLV_in_list = []
                DLV_out_list = []
                fz_list = []
                fz_pred_list = []
                CK_list = []
                miaoshu = 0

                if not (trigger.get(f'{machine_id}', {}).get(f'Tube_{tube}', None) is None):
                    diff_time = pd.Timestamp.now() - trigger[f'{machine_id}'][f'Tube_{tube}']
                    diff_time = pd.to_timedelta(diff_time)
                    miaoshu = diff_time.seconds

                print(f'秒数:{miaoshu}')

                if process_step == '进舟' and status == '1' and (f'Tube_{tube}' not in trigger[machine_id] or miaoshu >= 3600):
                    ####################################################################################################
                    try:
                        # 循环读取读取当前机台，当前管输入：列表
                        for j in range(len(loop_name)):
                            myvar = client.get_node('ns=2;s=Tag.Tube' + str(tube) + opc_tag[j])
                            DLV_in = myvar.get_value()
                            DLV_in_list.append(DLV_in)
                    except:
                        print(f'{machine_id}号机台{tube}号管输入获取失败！！！')
                        continue

                    # 获取整体膜厚数据
                    mohou_data = qury_mohou()
                    try:
                        fz_list = qury_fz(machine_id, tube)
                    except:
                        print(f'{machine_id}号机台{tube}号管方阻表不存在！！！')
                        continue
                    ################################################################################################
                    try:
                        # 连接机台获取舟号：单值
                        zhouhao = client.get_node(f'ns=2;s=Tag.Loader.stTube_BoatInfo{tube}.ID')
                        boat_id = zhouhao.get_value()
                    except:
                        print(f'{machine_id}号机台{tube}号管管号获取失败！！！')
                        continue
                    ################################################################################################
                    # 初始化只计算保存CK
                    if run_once.get(f'{machine_id}', {}).get(f'Tube_{tube}', None) is None or DLV_in_out[f'{machine_id}'][f'Tube_{tube}'] != DLV_out_list:
                        # 保存当前机台输入，作为调整下限
                        DLV_LL_to_database(machine_id, tube, DLV_out_LL, DLV_in_list)
                        for i in range(len(loop_name)):
                            CK_list.insert(i, para_init(fz_T[i], mh_T[i], DLV_in_list[i], A_r[i], A_mh[i]))

                        print('Feedforward-Initialize')
                        print(CK_list)
                        # 防重复计算
                        trigger[f'{machine_id}'][f'Tube_{tube}'] = pd.Timestamp.now()

                        # 初始化改值和进舟防重复计算改值
                        run_once[f'{machine_id}'][f'Tube_{tube}'] = 1

                        Feedforward_status = 'Initialize'

                        CK_to_database(machine_id, tube, CK_name, CK_list)

                        mh_fz_to_database(machine_id, tube, mh_name, mh_list, fz_name, fz_list, CK_name,
                                          CK_list,
                                          DLV_in_name, DLV_in_list, DLV_out_name, DLV_out_list, mh_count_name,
                                          mh_count_list,
                                          fz_pred_name, fz_pred_list, Feedforward_status)
                    ################################################################################################
                    # 前馈计算
                    elif run_once[f'{machine_id}'][f'Tube_{tube}'] == 1:
                        x_low = qury_DLV_in_LL(machine_id, tube)
                        x_low = [float(x) for x in x_low]
                        CK_list = qury_CK(machine_id, tube)  # 放在循环外面
                        for i in range(len(loop_name)):
                            # 获取膜厚和片数：单值
                            mh, mh_count = Aggregate_mh(machine_id, tube, mh_T[i], boat_id, begin_pos[i],
                                                        end_pos[i],
                                                        key_boat[i],
                                                        mohou_data)
                            mh_list.insert(i, mh)
                            mh_count_list.insert(i, mh_count)

                            fz_pred, DLV_out = mh_feedforward(fz_T[i], mh_list[i], mh_count_list[i], mh_T[i],
                                                              DLV_in_list[i], CK_list[i],
                                                              A_r[i], A_mh[i], x_max[i], y_low[i], y_up[i],
                                                              pianshu_LL[i], x_low[i], x_up[i])

                            DLV_out_list.insert(i, DLV_out)
                            fz_pred_list.insert(i, fz_pred)

                        # 防重复计算
                        trigger[f'{machine_id}'][f'Tube_{tube}'] = pd.Timestamp.now()

                        DLV_in_out[f'{machine_id}'][f'Tube_{tube}'] = DLV_out_list

                        Feedforward_status = 'Execute'

                        # 数据保存
                        mh_fz_to_database(machine_id, tube, mh_name, mh_list, fz_name, fz_list, CK_name,
                                          CK_list,
                                          DLV_in_name, DLV_in_list, DLV_out_name, DLV_out_list, mh_count_name,
                                          mh_count_list,
                                          fz_pred_name, fz_pred_list, Feedforward_status)
                        ################################################################################################
                        print('Feedforward-Execute')
                        print(DLV_in_list)
                        print(DLV_out_list)
                        print(mh_list)
                        print(fz_list)
                        print(fz_pred_list)
                        print(CK_list)
                        print(mh_count_list)
                        ################################################################################################
                        try:
                            # 命令下发
                            for k in range(len(loop_name)):
                                myvar1 = client.get_node('ns=2;s=Tag.Tube' + str(tube) + opc_tag[k])
                                myvar1.set_value(DLV_out_list[k])
                        except:
                            print(f'{machine_id}号机台{tube}号管命令下发失败！！！')
                        continue
                print(run_once)
                print(trigger)
            client.disconnect()
            print(f'{machine_id}号机台完成！！！')
        time.sleep(5)
