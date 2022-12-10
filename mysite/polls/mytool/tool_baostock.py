from . import tools
import pandas as pd
import baostock as bs


# 获取全部股k, save == "y":  # 是否保存
def baostock_history_k(dat, day, conn):

    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    # print('login respond  error_msg:'+lg.error_msg)
    """
    "日期" TEXT,
    "开盘" REAL,
    "收盘" REAL,
    "最高" REAL,
    "最低" REAL,
    "成交量" REAL,
    "成交额" REAL,
    "振幅" REAL,
    "涨跌幅" REAL,
    "涨跌额" REAL,
    "换手率" REAL
    """
    day2 = day.replace('/', '')
    for i, t in dat.iloc[0:].iterrows():
        print(i, t['name'].replace(' ', '').replace('*', ''), t['code'])
        code2 = tools.add_sh(t['code'], big="baostock")
        print(code2)

        # 获取沪深A股历史K线数据 1990-01-01
        rs = bs.query_history_k_data_plus(
            code2,
            """code,date,open,close,high,low,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST""",
            start_date='',
            end_date=day2,
            frequency="d",
            adjustflag="3"
        )
        # print('query_history_k_data_plus respond error_code:'+rs.error_code)
        # print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

        # 打印结果集 ####
        data_list = []
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            data_list.append(rs.get_row_data())
        result = pd.DataFrame(data_list, columns=rs.fields)
        result.insert(1, 'name', t['name'].replace(' ', '').replace('*', ''))
        # print(result)
        # 结果集输出到文件
        result.to_sql(
                    'baostock_day_k'+day,
                    con=conn,
                    if_exists='append',
                    index=False
                )
    # 登出系统 ####
    bs.logout()
