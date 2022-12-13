# import global_variable as gl_v
import baostock as bs
import pandas as pd


# 获取全部股k, save == "y":  # 是否保存
def baostock_history_k(save=''):

    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

    # 获取沪深A股历史K线数据 ####
    rs = bs.query_history_k_data_plus(
        'sh.600000',
        """date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST""",
        start_date='1990-01-01',
        end_date='2015-01-12',
        frequency="d",
        adjustflag="3"
    )
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

    # 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)

    # 结果集输出到文件
    # result.to_csv("D:\\history_A_stock_k_data.csv", index=False)
    print(result)

    # 登出系统 ####
    bs.logout()
    # conn, cur = gl_v.get_conn_cur()
    # conn.close()


baostock_history_k(save='')
