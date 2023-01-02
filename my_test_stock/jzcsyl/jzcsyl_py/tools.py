from time import time
from pathlib import Path
import sqlite3
import pandas as pd


def time_show(func):
    def new_func(*arg, **kw):
        t1 = time()
        res = func(*arg, **kw)
        t2 = time()
        # 前加f表示字符串内支持大括号内的python表达式
        print(f"{func.__name__: >10} : {t2-t1:.6f} sec")
        return res
    return new_func


def get_conn_cur(f=''):
    # db_path = "/home/chen/Documents/my220812/stock/my_stock_test.db"
    # db_path_poll =
    # "/home/chen/Documents/my220812/mysite/polls/polls_db.sqlite3"
    # db_path_my_school_test = """
    # /home/chen/Documents/my220812/stock/my_school_test/my_school_test.sqlite3"""
    db_path = Path(__file__).resolve().parent.parent.parent.parent
    if f == 'p':
        return db_path
    # print(db_path)  # /home/chen/Documents/my220812
    conn = sqlite3.connect(db_path / """mysite/polls/polls_db.sqlite3""")
    if f == '':
        return conn
    if f == 'conn_p':
        return conn, db_path


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


# 查询单个票后复权数据
def get_code_bfq(inp2, conn, d, fq=''):
    # 查询有baostock all表
    dat_t = pd.read_sql(
        """select name from sqlite_master where type='table' and name
        like '{}'""".format('baostock_day_k__________hfq'),
        conn
    )
    dat1 = pd.read_sql(
        r"""select close from '{}' where code='{}'
        and date='{}'""".format(dat_t.iloc[0]['name'], inp2, '2015-06-01'),
        conn
    )
    # print(dat1)
    li = [
        '2016-06-01',
        '2017-06-01',
        '2018-06-01',
        '2019-06-01',
        '2020-06-01',
        '2021-06-01',
        '2022-06-01',
    ]
    dd = pd.DataFrame()
    for ii in li:
        dat2 = pd.read_sql(
            r"""select close from '{}' where code='{}'
            and date>='{}' limit 0,1
            """.format(dat_t.iloc[0]['name'], inp2, ii),
            conn
        )
        if not dat2.empty:
            dd[ii] = dat2['close']
            # dd = pd.concat([dat2, dd], axis=0, ignore_index=True)
            # break
    # print(dd)
    dd = dd.astype(float)
    dat1 = float(dat1['close'])
    # print((dd-dat1)/dat1)
    # dat2.iloc[:, 1:] = dat2.iloc[:, 1:].replace('', 0).astype(float).round(2)
    # dat2.to_sql(inp2 + fq, con=conn, if_exists='replace', index=False)kk
    return ((dd-dat1)/dat1).round(4)
