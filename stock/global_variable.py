from time import time
from pathlib import Path
import sqlite3

# 银行500亿以上
bank_li = """sh601398,sz000001,sz002142,sh600000,sh600015,sh600016,sh600036,
sh600919,sh600926,sh601009,sh601166,sh601169,sh601229,sh601288,sh601328,
sh601658,sh601818,sh601838,sh601916,sh601939,sh601988,sh601998"""


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
