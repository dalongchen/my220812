import pandas as pd
from time import time


# big="baostock"加(sh. or sz.)code加(sh or sz) or (SZ or SH)
def add_sh(code, big=""):
    if big == "":
        if (code.startswith("0") or code.startswith("3")
                or code.startswith("2")):
            code = "sz" + code
        elif (code.startswith("5") or code.startswith("6")
                or code.startswith("9")):
            code = "sh" + code
        else:
            print("err1", code)
    elif big == "baostock":
        if (code.startswith("0") or code.startswith("3")
                or code.startswith("2")):
            code = "sz." + code
        elif (code.startswith("5") or code.startswith("6")
                or code.startswith("9")):
            code = "sh." + code
        else:
            print("err2", code)
    elif big == "east.":
        if (code.startswith("0") or code.startswith("3")
                or code.startswith("2")):
            code = "0." + code
        elif (code.startswith("5") or code.startswith("6")
                or code.startswith("9")):
            code = "1." + code
        else:
            print("err2", code)
    else:
        if (code.startswith("0") or code.startswith("3")
                or code.startswith("2")):
            code = "SZ" + code
        elif (code.startswith("5") or code.startswith("6")
                or code.startswith("9")):
            code = "SH" + code
        else:
            print("err3", code)
    return code


def time_show(func):
    def new_func(*arg, **kw):
        t1 = time()
        res = func(*arg, **kw)
        t2 = time()
        # 前加f表示字符串内支持大括号内的python表达式
        print(f"{func.__name__: >10} : {t2-t1:.6f} sec")
        return res
    return new_func


# 循环合并两df中的重复部分-concat
def new_stock_yjbb_em_20_concat(df_new2021, df_new2020):
    df_new_concat = pd.DataFrame()  # 循环合并两df中的重复部分
    for i, t in df_new2021.iterrows():
        # print(t['股票代码'])
        if t['股票代码'] in df_new2020:
            df_new_concat = pd.concat([df_new_concat, t], axis=1)
    df_new_concat = pd.DataFrame(df_new_concat.values.T,
                                 index=df_new_concat.columns,
                                 columns=df_new_concat.index)
    return df_new_concat


# 循环合并两df中的重复部分-delete
def new_stock_yjbb_em_20_delete(df_new2021, df_new2020):
    df_new_concat = df_new2021.copy()
    # print(df_new_concat)
    for i, t in df_new2021.iterrows():
        if t['股票代码'] not in df_new2020:
            # print(i, t)
            df_new_concat.drop(index=[i], inplace=True)

    return df_new_concat


# add column,循环合并两df中的重复部分-delete
def add_column_concat_delete(df_new2021, df_new2020):
    df_new_concat = df_new2021.copy()
    # print(df_new_concat)
    for i, t in df_new2021.iterrows():
        if t['股票代码'] not in df_new2020['股票代码'].values:
            # print(i, t)
            df_new_concat.drop(index=[i], inplace=True)
    # print(df_new_concat)
    df_ = df_new2020.copy()
    for i, t in df_new2020.iterrows():
        if t['股票代码'] not in df_new2021['股票代码'].values:
            # print(i, t)
            df_.drop(index=[i], inplace=True)
    d = pd.to_numeric(df_['总资产收益率'], errors='coerce')*100
    # print(d.round(2))
    df_new_concat['总资产收益率'] = (d.round(2)).values
    return df_new_concat


# 获取季度数组
def get_quarter_array(f='', day=''):
    import datetime
    if f == '前五年' and day:
        now_year = int(day[:4])
        m = int(day[5:7])
        # print(day, now_year, '---------', m)
        arr_quater, xx = get_quarter_array_son(now_year, m, 5)
        # print(xx, arr_quater[xx:])
        return arr_quater[xx:]
    if f == '':
        now = datetime.datetime.now()
        # now = datetime.datetime.now().strftime('%Y-%m-%d')
        now_year = now.year
        m = now.month
        _range = now_year - 1988
        # 子函数
        arr_quater, xx = get_quarter_array_son(now_year, m, _range)
        # print(arr_quater[xx:-3])
        return arr_quater[xx:-3]


# 子函数
def get_quarter_array_son(now_year, m, _range):
    if m > 0 and m < 4:
        xx = 4
    if m > 3 and m < 7:
        xx = 3
    if m > 6 and m < 10:
        xx = 2
    if m > 9 and m < 13:
        xx = 1
        # print(m, xx)
    arr_quater = []
    _quater = ['1231', '0930', '0630', '0331']
    for ii in range(_range):
        y = str(now_year-ii)
        for x in _quater:
            arr_quater.append(y + x)
    return arr_quater, xx
