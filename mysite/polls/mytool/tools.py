import pandas as pd
from . import tool_db
from time import time


def stockk0(inp):  # 不复权
    conn, cur = tool_db.get_conn_cur()
    sql = r"""select 日期,收盘价,开盘价,最低价,最高价,成交量 from {} where
    成交量 != 0""".format(inp)
    dat2 = pd.read_sql(sql, conn)
    conn.close()
    dat2 = dat2.head(3)
    # print(dat2)
    dat1_ = dat2.iloc[:, 1:]
    dat1_pre = dat1_.values  # 未改变前的值
    dat1_.insert(4, 'i', dat1_.index.tolist())
    dat1_['max'] = dat1_.apply(
        lambda x: 1 if x['收盘价'] > x['开盘价'] else -1, axis=1)
    # print(dat1_.iloc[:,4:].values)
    dat3 = {'categoryData': dat2.日期.values.tolist(), 'values': dat1_pre.tolist(
    ), 'volumes': dat1_.iloc[:, 4:].values.tolist()}
    return dat3


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
def get_quarter_array():
    return [
        '20220630',
        '20211231',
        '20210630',
        '20201231',
        '20200630',
        '20191231',
        '20190630',
        '20181231',
        '20180630',
        '20171231',
        '20170630',
        '20161231',
        '20160630',
        '20151231',
        '20150630',
        '20141231',
        '20140630',
        '20131231',
        '20130630',
        '20121231',
        '20120630',
        '20111231',
        '20110630',
        '20101231',
        '20100630',

        '20091231',
        '20090630',
        '20081231',
        '20080630',
        '20071231',
        '20070630',
        '20061231',
        '20060630',
        '20051231',
        '20050630',
        '20041231',
        '20040630',
        '20031231',
        '20030630',
        '20021231',
        '20020630',
        '20011231',
        '20010630',
        '20001231',
        '20000630',

        '19991231',
        '19990630',
        '19981231',
        '19980630',
        '19971231',
        '19970630',
        '19961231',
        '19960630',
        '19951231',
        '19950630',
        '19941231',
        '19940630',
        '19931231',
        '19930630',
        '19921231',
        '19920630',
        '19911231',
        '19901231'
    ]
