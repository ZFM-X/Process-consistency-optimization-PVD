#!/usr/bin/python3.8.4 (python版本)
# -*- coding: utf-8 -*-
# @Software: PyCharm
# @Time    : 2024/1/9 10:18
# @File    : caculate.py
# ************************************************* 定义函数计算各参数 ************************************************** #
# ################################################ 膜厚-方阻参数初始化 ################################################## #
def para_init(fz_T, mh_T, DLV_in, A_r, A_mh) -> float:
    """
    定义方阻反馈初始化函数
    :param fz_T: 目标方阻
    :param mh_T: 目标膜厚
    :param DLV_in: 输入
    :param A_r: 温度增益
    :param A_mh: 膜厚增益
    :return: 初始化CK
    """
    CK = fz_T - A_r * DLV_in - A_mh * mh_T
    return CK


# ##################################################### 方阻R2R ###################################################### #
def fz_R2R(fz_T, fz, mh_T, DLV_in, CK_1, W, A_r, A_mh):
    """
    定义正常方阻R2R函数
    :param fz: 当前方阻
    :param mh_T: 目标膜厚
    :param DLV_in: 输入
    :param CK_1: 上批CK
    :param W: 权重
    :param A_r: 温度增益
    :param A_mh: 膜厚增益
    :return: 更新后CK
    """
    CK = W * (fz - A_r * DLV_in - A_mh * mh_T) + (1 - W) * CK_1
    DLV_out = (fz_T - A_mh * mh_T - CK) / A_r
    DLV_out = round(DLV_out)

    return CK, DLV_out


# ################################################# 膜厚feedforward ################################################## #
def mh_feedforward(fz_T, mh, mh_count, mh_T, DLV_in, CK, A_r, A_mh, max_change, adjust_lL, adjust_HL, pianshu_LL,
                   DLV_out_LSL, DLV_out_USL):
    """
    定义正常方阻R2R函数
    :param fz_T: 目标方阻
    :param mh: 当前膜厚
    :param mh_count: 膜厚片数
    :param DLV_in: 输入
    :param CK: 本批CK
    :param A_r: 温度增益
    :param A_mh: 膜厚增益
    :param max_change: 最大调整
    :param adjust_lL: 不用调时下限
    :param adjust_HL: 不用调时上限
    :param pianshu_LL: 调整片数下限
    :param DLV_out_USL: 输出上限
    :param DLV_out_LSL: 输出下限
    :return: 调整输出，预测方阻
    """
    if mh_count < pianshu_LL:
        fz_pred = A_r * DLV_in + A_mh * mh_T + CK
    else:
        fz_pred = A_r * DLV_in + A_mh * mh + CK

    if mh == mh_T or (adjust_lL <= fz_pred <= adjust_HL):
        DLV_out = DLV_in
    else:
        Xout = (fz_T - A_mh * mh - CK) / A_r

        if Xout - DLV_in > max_change:
            ROC = DLV_in + max_change
        elif Xout - DLV_in < - max_change:
            ROC = DLV_in - max_change
        else:
            ROC = Xout

        # 根据，计算输出Xout，计算实际输出DLV_out
        if ROC > DLV_out_USL:
            DLV_out = DLV_out_USL
        elif ROC < DLV_out_LSL:
            DLV_out = DLV_out_LSL
        else:
            DLV_out = ROC
    DLV_out = round(DLV_out)

    return fz_pred, DLV_out
