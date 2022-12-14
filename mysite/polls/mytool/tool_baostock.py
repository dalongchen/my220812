from . import tools
import pandas as pd
import baostock as bs


# 获取全部股k  # 是否保存
def baostock_history_k(dat, day, conn):
    import datetime
    # from datetime import datetime
    # 查询多少个表，查询已经记录的ｋ线最后日期
    dat_t = pd.read_sql(
        """select name from sqlite_master where type='table' and
            name like '{}'""".format('baostock_day_k%'),
        conn
    )
    start_d = dat_t['name'].values[-1][14:]
    start_d = datetime.datetime.strptime(start_d, '%Y-%m-%d')
    start_d += datetime.timedelta(days=1)
    start_d = str(start_d)[:10]
    print(start_d)
    # print()
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    # print('login respond  error_msg:'+lg.error_msg)
    day2 = day.replace('/', '-')
    for i, t in dat.iloc[0:].iterrows():
        print(i, t['name'].replace(' ', '').replace('*', ''), t['code'])
        code2 = tools.add_sh(t['code'], big="baostock")
        # print(code2, day2)
        # return
        # 获取沪深A股历史K线数据 1990-01-01
        rs = bs.query_history_k_data_plus(
            code2,
            """code,date,open,close,high,low,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST""",
            start_date=start_d,
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
        # return
        # 结果集输出到文件
        result.to_sql(
            'baostock_day_k'+day2,
            con=conn,
            if_exists='append',
            index=False
        )
    # 登出系统 ####
    bs.logout()
