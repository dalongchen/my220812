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
    print(db_path)
    conn = sqlite3.connect(db_path)
    return conn


# get_conn_cur()


def history_k_single(name2, code2, conn='', save='',
                     end_date='', fq=''):  # 获取数据并保存数据库
    import akshare as ak
    """
    目标地址: http://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)
    描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新,
    限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
    """

    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code2, period="daily",
                                            start_date='',  end_date=end_date,
                                            adjust=fq)
    # print(stock_zh_a_hist_df)
    if save == 'y':
        if conn == '':
            from . import tool_db
            conn, cur = tool_db.get_conn_cur()
            stock_zh_a_hist_df.to_sql(
                name2+code2+fq, con=conn, if_exists='replace', index=False)
            conn.commit()
            conn.close()
        else:
            stock_zh_a_hist_df.to_sql(
                name2+code2+fq, con=conn, if_exists='replace', index=False)
            conn.commit()


# 获取季度数组
def get_quarter_array(f=''):
    import datetime
    if f == 'year':
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
    if f == 'year_middle':

        now = datetime.datetime.now()
        # now = datetime.datetime.now().strftime('%Y-%m-%d')
        # xx = 0
        m = now.month
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
        for ii in range(now.year-1988):
            y = str(now.year-ii)
            for x in _quater:
                arr_quater.append(y + x)
        # print(arr_quater[xx:-3])
        return arr_quater[xx:-3]
