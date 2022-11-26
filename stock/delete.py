import pandas as pd


def test():
    df2 = pd.DataFrame()
    df2['q'] = [39.55, 41.41, 46.55, 51.72, 26.09, 27.52, 26.28]
    df2['h'] = [41.41, 46.55, 51.72, 26.09, 27.52, 26.28, 24.33]
    df2['z'] = [4.7, 12.41, 11.11, -5.41, 5.48, -4.51, -7.42]
    df2["f"] = (df2["z"]/100+1).cumprod()
    print(df2)
    df2["后复权收盘价"] = 39.55*df2["f"]
    print(df2)


def test2():
    import numpy as np
    dfc = pd.DataFrame({
        'a': ['one', 'one', 'two', 'three', 'two', 'one', 'six'],
        'c': np.arange(7)})
    dfd = dfc.copy()
    # Setting multiple items using a mask
    mask = dfd['a'].str.startswith('o')
    # breakpoint()
    dfd.loc[mask, 'c'] = 42
    print(dfd)


test2()
# def test_hfq():
#     # 计算后复权值
#     df['hfqyinzi']=(df['chgPct']+1).cumprod()
#     initial_price = df.iloc[0]['closePrice']/(1+df.iloc[0]['chgPct'])
#     df['closePrice_hfq']=initial_price*df.hfqyinzi


# @gl_v.time_show  # 计算复权因子
# def fq_factor(tab, save=''):
#     import pandas as pd
#     conn, cur = gl_v.get_conn_cur()
#     sql_s = r"""select * from {}"""
#     s = pd.read_sql(sql_s.format(tab), conn)
#     # 计算复权因子
#     # print(s["涨跌幅"]/100+1)
#     s["复权因子"] = (s["涨跌幅"]/100+1).cumprod()
#     print(s.iloc[0]["收盘"], s.iloc[0]["复权因子"])
#     scale = s.iloc[0]["收盘"]/s.iloc[0]["复权因子"]
#     s["后复收盘"] = scale*s["复权因子"]
#     print(s)
#     if save == 'y':
#         s.to_sql(tab, con=conn,
#                  if_exists='replace', index=False)
#     conn.close()


# fq_factor('万科A000002', save='y')
# fq_factor('美的集团000333', save='y')


# @gl_v.time_show  # 计算复权因子
# def fq_factor2(tab, save=''):
#     import pandas as pd
#     conn, cur = gl_v.get_conn_cur()
#     sql_s = r"""select * from {}"""
#     df = pd.read_sql(sql_s.format(tab), conn)
#     # 计算复权因子
#     # df['hfqyinzi'] = (df['涨跌幅']+1).cumprod()
#     df['hfqyinzi'] = ((df['涨跌幅']/100)+1).cumprod()
#     # initial_price = df.iloc[0]['收盘']/(1+df.iloc[0]['涨跌幅'])
#     initial_price = df.iloc[0]['收盘']/(1+df.iloc[0]['涨跌幅']/100)
#     # initial_price = df.iloc[0]['closePrice']/(1+df.iloc[0]['chgPct'])
#     df['closePrice_hfq'] = initial_price*df.hfqyinzi
#     print(df)
#     if save == 'y':
#         df.to_sql(tab, con=conn,
#                   if_exists='replace', index=False)
#     conn.close()


# # fq_factor2('天齐锂业002466', save='')
# fq_factor2('美的集团000333', save='y')