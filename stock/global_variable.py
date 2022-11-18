from time import time
from pathlib import Path
import sqlite3


def time_show(func):
    def new_func(*arg, **kw):
        t1 = time()
        res = func(*arg, **kw)
        t2 = time()
        # 前加f表示字符串内支持大括号内的python表达式
        print(f"{func.__name__: >10} : {t2-t1:.6f} sec")
        return res
    return new_func


def get_conn_cur():
    db_path = Path(__file__).resolve().parent.parent
    db_path = db_path / """mysite/polls/polls_db.sqlite3"""
    # print(db_path)
    conn = sqlite3.connect(db_path)
    return conn, conn.cursor()


# get_conn_cur()


def history_k_single(name2, code2, conn='', save='',
                     end_date='20221115', fq='hfq'):  # 获取数据并保存数据库
    import akshare as ak

    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code2, period="daily",
                                            start_date='',  end_date=end_date,
                                            adjust=fq)
    # print(stock_zh_a_hist_df)
    if save == 'y':
        if conn == '':
            from . import tool_db
            conn, cur = tool_db.get_conn_cur()
            stock_zh_a_hist_df.to_sql(
                name2+code2+'hfq', con=conn, if_exists='replace', index=False)
            conn.commit()
            conn.close()
        else:
            stock_zh_a_hist_df.to_sql(
                name2+code2+'hfq', con=conn, if_exists='replace', index=False)
            conn.commit()


# 获取季度数组
def get_quarter_array():
    return [
        '20211231',
        '20201231',
        '20191231',
        '20181231',
        '20171231',
        '20161231',
        '20151231',
        '20141231',
        '20131231',
        '20121231',
        '20111231',
        '20101231',
        '20091231',
        '20081231',
        '20071231',
        '20061231',
        '20051231',
        '20041231',
        '20031231',
        '20021231',
        '20011231',
        '20001231'
    ]
