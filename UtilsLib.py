import pandas as pd
import pymysql
from collections import Counter
import math
# from FeatureLibrary.BaseFeature import Data_Factory
import numpy as np
import time
from pyhdfs import HdfsClient
from functools import wraps
import os
from numba import jit

hdfs_url = "hdfs://192.168.30.201:8020"
hdfs_client = HdfsClient(hosts=['192.168.30.201:9870', '192.168.30.202:9870'], user_name="worker")


def delete_hdfs_dir(hdfs_path):
    """
    删除hdfs非空目录(目录下没有子目录)
    :param hdfs_path:
    :return:
    """
    for f in hdfs_client.listdir(hdfs_path):
        hdfs_file_path = os.path.join(hdfs_path, f)
        hdfs_client.delete(hdfs_file_path)
    hdfs_client.delete(hdfs_path)


def get_timeframe_primary_keys_dict():
    """
    获取不同时间级别主键字典
    :param timeframe_list:
    :return:
    """
    timeframe_list = ['1min', '5min', '15min', '30min', '60min', 'day', 'day_snapshot', 'week', 'week_snapshot', "tick"]
    timeframe_primary_keys_dict = {}
    for timeframe in timeframe_list:
        if 'min' in timeframe or timeframe in ['day_snapshot', 'tick']:
            timeframe_primary_keys_dict[timeframe] = ['Stock', 'Date', 'Time']
        else:
            timeframe_primary_keys_dict[timeframe] = ['Stock', 'Date']
    return timeframe_primary_keys_dict


def unequal_line_primarykeys_keypoint(df1, df2, primary_keys, n=1):
    """
    两个df,从上至下进行比对,返回起始行开始前n行不同行所对应的primary_key
    :param df1:
    :param df2:
    :param primary_keys:
    :return:
    """

    if df1.shape != df2.shape:
        print("比较数据行列数不一致!")

    df1 = df1.replace(np.nan, 0).replace(np.inf, 1)
    df2 = df2.replace(np.nan, 0).replace(np.inf, 1)
    df2 = df2[df1.columns]
    compare_df = df1 == df2
    for i in range(len(compare_df)):
        if len(set(compare_df.iloc[i].values)) != 1:
            print("数据相同最后" + str(n) + "行索引:\n", df1.iloc[max(0, i - n):i][primary_keys].reset_index(drop=True))
            print("数据不同前" + str(n) + "行索引:\n", df1.iloc[i:i + n][primary_keys].reset_index(drop=True))
            return
    print("数据完全相同!")


def shuffle_df(df, unshuffle_columns, start_shuffle_date=None, start_shuffle_time=None):
    """
    数据随机化
    :param df: 原数据
    :param unshuffle_columns: 不进行随机化的列名
    :param start_shuffle_date: 起始日期
    :param start_shuffle_time: 起始时间
    :return:
    """
    shuffle_df = df.copy()
    if start_shuffle_date:
        if "Time" in shuffle_df.columns and start_shuffle_time:
            mask = (shuffle_df.Date >= start_shuffle_date) & (shuffle_df.Time > start_shuffle_time)
        else:
            mask = shuffle_df.Date >= start_shuffle_date
        feature_cols = [col for col in shuffle_df.columns if col not in unshuffle_columns]
        shape1 = shuffle_df.loc[mask, feature_cols].shape
        random_matrix = np.random.normal(0, 1, shape1)
        shuffle_df.loc[mask, feature_cols] = random_matrix
    return shuffle_df


def run_time(info):
    """
    函数运行时间
    :param func:
    :return:
    """

    def decorator(func):
        @wraps(func)
        def decorated(*args, **kwargs):
            start = time.time()
            log_info(info, "start...")
            res = func(*args, **kwargs)
            diff_time = time.time() - start
            h = int(diff_time // 3600)
            m = int(diff_time - diff_time // 3600 * 3600) // 60
            s = round(diff_time - h * 3600 - m * 60, 3)
            time_arr = [str(t) for t in [h, m, s]]
            # index = 0
            # for t in time_arr:
            #     if float(t) == 0:
            #         index += 1
            #         continue
            #     else:
            #         break
            # time_s = ":".join(time_arr[index:])
            time_s = ":".join(time_arr)

            log_info(info, f"finished!\nruning time:{time_s}")
            return res

        return decorated

    return decorator

def unix2time(timestamp):
    """
    unix时间转换成time
    :param timestamp: float/int
    :return: float/int
    """
    time_struct = time.localtime(timestamp//1)
    return int(time.strftime("%Y%m%d%H%M%S",time_struct)) + timestamp%1


def bstr_gbk_encoding(s):
    """
    byte str 进行gbk转换
    :return: s
    """
    return str(s,encoding="gbk")

def datetime2unix(date,time_):
    """
    日期+时间转unix时间
    :param date:(eg:20180102)
    :param time_:(eg:90019000.123)/ms
    :return:/s
    """
    ms = time_%1000/1000
    time_ = str(time_//1000 + 10**6)[1:]
    time_str = str(date) + time_
    timeArray = time.strptime(time_str, "%Y%m%d%H%M%S")
    timestamp = time.mktime(timeArray) + ms
    return timestamp

def time2unix(time_):
    """
    时间转unix时间
    :param time_: (eg:90019000.123)/ms
    :return: /s
    """
    time_ = time_ / 1000
    s = time_ // 10000 * 3600 + (time_ - time_ // 10000 * 10000) // 100 * 60 + time_ % 100
    return s


def get_now_time_float():
    """
    :return: time_num(eg:20190101133101.xxxx/s),now(unix时间秒)
    """
    now = time.time()
    timeArray = time.localtime(now)
    time_num = int(time.strftime("%Y%m%d%H%M%S", timeArray)) + now%1
    return time_num,now

def time2min(t):
    """
    time时间转换成1分钟,秒向后取整
    :param t: time(eg:90733000.00/ms)
    :return:
    """
    mintime = math.ceil(t/100000)
    #0时
    if mintime == 2360:
        return 0
    #整时
    h = mintime//100
    min_ = mintime - mintime//100*100
    if min_ == 60:
        mintime = (h+1)*100
    return mintime

def time_minus_n_min(time, n_min):
    """
    时间减分钟数
    :param time: eg:1030
    :param n_min: eg:60
    :return: eg:1030-60=930
    """
    # 转化为多少分钟数
    time = time // 100 * 60 + time % 100 - n_min
    time = time // 60 * 100 + time % 60

    return time


# def get_stock_list(stock_index,start_date,end_date):
#     """
#     获取股票池股票列表 eg:‘000300.SH__000905.SH__000852.SH’
#     :return: list
#     """
#     data_fac = Data_Factory()
#
#     if "__" in stock_index:
#         stocks_df = [data_fac.get_data(
#             spn
#             ,start_date
#             ,end_date
#             ,"index_stock_pool"
#         ) for spn in stock_index.split("__")]
#         stocks_df = pd.concat(stocks_df,ignore_index=True)
#     else:
#         stocks_df = data_fac.get_data(stock_index,start_date,end_date,"index_stock_pool")
#
#     stock_list = [s for s in list(set(stocks_df.values.flat)) if type(s)==str]
#     return stock_list

def round_45(_float, _len):
    """
    四舍五入
    :param _float:
    :param _len:
    :return:
    """
    if isinstance(_float, float):
        if str(_float)[::-1].find('.') <= _len:
            return (_float)
        if str(_float)[-1] == '5':
            return (round(float(str(_float)[:-1] + '6'), _len))
        else:
            return (round(_float, _len))
    else:
        return (round(_float, _len))


def limit_up_px(pre_close, persent=0.1):
    """
    涨停板价格:(前一收盘价*1.1)*四舍五入
    :param pre_close:前一收盘价
    :param persent:涨停幅度百分比
    :return:涨停板价格
    """

    return round_45(pre_close * (1 + persent), 2)


def limit_down_px(pre_close, persent=0.1):
    """
    跌停析价格:(前一收盘价*0.9)*四舍五入
    :param pre_close: 
    :param persent: 
    :return: 
    """
    return round_45(pre_close * (1 - persent), 2)


def diff_set(list1, list2):
    """
    列表求差集
    重复项{重复值:重复次数统计} 重复次数>=2
    :param list1:
    :param list2:
    :return: {0:set1-set2,1:set2-set1}
    """
    # 统计重复项
    for i, l in enumerate([list1, list2]):
        gt2l = {k: v for k, v in Counter(l).items() if v > 1}
        print("列表" + str(i + 1) + "重复项计数:", gt2l)
    s1 = set(list1)
    s2 = set(list2)
    s1_2 = s1 - s2
    s2_1 = s2 - s1

    return {k: v for k, v in enumerate([s1_2, s2_1])}


def df_to_mysql_tonydb(df, mysql_table_name):
    """
    :param df:
    :param mysql_table_name:
    :return:
    """
    df.to_sql(name=mysql_table_name, con='mysql+pymysql://tonydb:nm90NM()@192.168.30.201:3306/tonydb?charset=utf8',
              if_exists='replace', index=False)


def mysql_tonydb_to_df(mysql_table_name):
    """
    :param mysql_table_name:tonydb库中mysql表
    :return:
    """
    conn = pymysql.connect(
        host='192.168.30.201',
        user='tonydb',
        passwd='nm90NM()',
        db='tonydb',
        port=3306,
        charset='utf8'
    )
    df = pd.read_sql('select * from ' + mysql_table_name, conn)
    conn.close()
    return df


def split_steps(length, n):
    """
    length步长拆分成n份,前后步形成list
    eg:
    split_steps(5,3)
    [[0, 2], [2, 4], [4, 5]]
    """

    step = list(range(0, length, math.ceil(length / n)))
    step.append(length)

    steps = []
    for i in range(len(step)):
        if i != len(step) - 1:
            steps.append([step[i], step[i + 1]])
    return steps


def get_primary_keys(timeframe):
    """
    由timeframe返回primary_keys
    :param timeframe:
    :return:
    """
    if 'min' in timeframe or timeframe in ['day_snapshot', 'tick']:
        return ['Stock', 'Date', 'Time']
    else:
        return ['Stock', 'Date']


def get_prim_key_cols_dict(timeframe_list):
    """
    获取不同时间级别主键字典
    :param timeframe_list:
    :return:
    """
    prim_key_cols_dict = {}
    for timeframe in timeframe_list:
        if 'min' in timeframe or timeframe in ['day_snapshot', 'tick']:
            prim_key_cols_dict[timeframe] = ['Stock', 'Date', 'Time']
        else:
            prim_key_cols_dict[timeframe] = ['Stock', 'Date']
    return prim_key_cols_dict


def get_time_int():
    now = time.time()
    timeArray = time.localtime(now)
    return int(time.strftime("%Y%m%d%H%M%S", timeArray))


def time_str():
    now = time.time()
    timeArray = time.localtime(now)
    return time.strftime("%Y/%m/%d %H:%M:%S", timeArray)


def log_info(info, *args):
    print(time_str() + " [INFO] ", info, *args)


def log_error(info, *args):
    print(time_str() + " [ERROR] ", info, *args)


def log_warnning(info, *args):
    print(time_str() + " [WARNNING] ", info, *args)


@jit(nopython=True)
def timeframe_60min(_time):
    """
    60min区间划分
    """
    if _time <= 1030:
        return 1030
    elif _time <= 1130:
        return 1130
    elif _time <= 1400:
        return 1400
    else:
        return 1500

@jit(nopython=True)
def timeframe_5min(_time):
    """
    5min区间划分
    """
    if _time <= 935:
        return 935
    else:
        res = math.ceil(_time / 5) * 5
        m = res % 100
        if m == 60:
            res = (res // 100 + 1) * 100
        return res


@jit(nopython=True)
def timeframe_15min(_time):
    """
    15min区间划分
    """
    if _time <= 945:
        return 945
    else:
        m = _time % 100
        h = _time // 100

        m = math.ceil(m / 15) * 15
        if m == 60:
            res = (h + 1) * 100
        else:
            res = h * 100 + m
        return res


@run_time("function test")
def test():
    """
    function test
    :return:
    """
    print(timeframe_15min(945))


if __name__ == '__main__':
    # 四舍五入测试
    # print(round_45(1.135,2))
    # print(round_45(1.125,2))

    # 涨停板test

    # pc = 27.51
    # print(pc,limit_up_px(pc))

    # 跌停板
    # pc = 28.98
    #
    # print(pc,limit_down_px(pc))

    # 差集
    # list1 = [1,1,2]
    # list2 = [2,3,3]
    # print((diff_set(list1,list2)))

    # print(split_steps(2647, 2647//1000+1))
    # print(get_time_int())
    test()

    pass
