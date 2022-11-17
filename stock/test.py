import sqlite3
import global_variable as gl_v


@gl_v.time_show
def history_volatility(db='',
                       sql="select 日期,涨跌幅 from 'df601398hfq'"):
    # 连接数据库，如果不存在，则自动创建
    conn = sqlite3.connect(db)
    # 创建游标
    cur = conn.cursor()
    cu = cur.execute(sql)
    print(cu.fetchone())
    # 关闭游标
    cur.close()
    # 断开数据库连接
    conn.close()
    return cu

# history_volatility(gl_v.db_path_poll)


@gl_v.time_show
def history_test(last_day, save=''):
    import akshare as ak
    import time

    # stock_sector_detail_df = ak.stock_board_industry_name_ths()
    # print(stock_sector_detail_df)

    # 获取银行成分股
    stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(
        symbol="银行")
    print(stock_board_industry_cons_ths_df[["代码", "名称"]])
    conn = sqlite3.connect(gl_v.db_path_poll)
    cur = conn.cursor()
    for a in stock_board_industry_cons_ths_df[["代码", "名称"]].values:
        print(a[0], a[1])
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=a[0], period="daily",
                                                start_date='',
                                                end_date=last_day,
                                                adjust="hfq")
        # print(stock_zh_a_hist_df)
        if save == 'y':
            stock_zh_a_hist_df.to_sql(
                a[1]+a[0]+'hfq', con=conn, if_exists='replace', index=False)
        time.sleep(1.23)

    cur.close()
    conn.close()

# history_test(last_day='20221105',save='y')


@gl_v.time_show
def history_divident_test(save=''):  # 下载股票分红
    import akshare as ak
    import time

    # 获取银行成分股
    stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(
        symbol="银行")
    print(stock_board_industry_cons_ths_df[["代码", "名称"]])
    conn = sqlite3.connect(gl_v.db_path_poll)
    cur = conn.cursor()
    for a in stock_board_industry_cons_ths_df[["代码", "名称"]].values:
        print(a[0], a[1])
        stock_dividents_cninfo_df = ak.stock_dividents_cninfo(symbol=a[0])
        # print(stock_dividents_cninfo_df)
        if save == 'y':
            stock_dividents_cninfo_df.to_sql(
                a[1]+a[0]+'分红', con=conn, if_exists='replace', index=False)
        time.sleep(1.23)
    cur.close()
    conn.close()
# history_divident_test(save ='y')


@gl_v.time_show
def history_divident_test_local():  # 分析股票分红
    import pandas as pd
    conn = sqlite3.connect(gl_v.db_path_poll)
    # cur=conn.cursor()
    li = [
        '交通银行601328分红',
        '中国银行601988分红',
        '建设银行601939分红',
        '工商银行601398分红',
        '农业银行601288分红'
    ]
    sql = r"""select * from {}"""
    for a in li:
        dat2 = pd.read_sql(sql.format(a), conn)
        print(a, dat2['派息比例'].sum())
    # cur.close()
    conn.close()


# history_divident_test_local()


@gl_v.time_show
def stock_a_lg_indicator(save=''):  # 下载市盈市净股息
    import akshare as ak
    import time

    # 获取银行成分股
    stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(
        symbol="银行")
    print(stock_board_industry_cons_ths_df[["代码", "名称"]])
    conn = sqlite3.connect(gl_v.db_path_poll)
    cur = conn.cursor()
    for a in stock_board_industry_cons_ths_df[["代码", "名称"]].values:
        print(a[0], a[1])
        stock_a_lg_indicator_df = ak.stock_a_lg_indicator(symbol=a[0])
        # print(stock_a_lg_indicator_df)
        if save == 'y':
            stock_a_lg_indicator_df.to_sql(
                a[1]+a[0]+'市盈市净股息', con=conn, if_exists='replace', index=False)
        time.sleep(1.23)
    cur.close()
    conn.close()
# stock_a_lg_indicator(save='y')


@gl_v.time_show
def stock_a_lg_indicator_local():  # 分析市盈市净股息
    import pandas as pd
    conn = sqlite3.connect(gl_v.db_path_poll)
    # cur=conn.cursor()
    li = [
        '交通银行601328市盈市净股息',
        '中国银行601988市盈市净股息',
        '建设银行601939市盈市净股息',
        '工商银行601398市盈市净股息',
        '农业银行601288市盈市净股息'
    ]
    # sql = r"""select * from {}"""
    sql = r"""select dv_ratio from {} ORDER BY trade_date desc limit 244*4"""
    for a in li:
        dat2 = pd.read_sql(sql.format(a), conn)
        print(a, dat2.mean())
    # cur.close()
    conn.close()

# stock_a_lg_indicator_local()


@gl_v.time_show
def history_m_table_name():
    # import akshare as ak
    # import time
    conn, cur = gl_v.get_conn_cur()

    # 查询有没有这个表
    sql_tab_name = """select name from sqlite_master where type='table' and
    name like '{}%'"""
    cu = cur.execute(sql_tab_name.format('east'))  # 查询有没有这个表
    # 查询股票中文名
    sql_china_name = """select name from stock_info_a_code_name
     where code='{}'"""
    sql_rename = """ALTER TABLE '{}' RENAME TO {}"""
    for t in cu.fetchall():
        # print(t[0])
        cu_name = cur.execute(sql_china_name.format(t[0][-8:-2])).fetchone()
        # print(cu_name.fetchone())
        # cu_name = cu_name.fetchone()
        # print(cu_name)
        if cu_name:
            if '*' not in cu_name[0]:
                print(cu_name[0] + t[0][-8:-2] + 'hfq')
                try:
                    cur.execute(sql_rename.format(
                        t[0],
                        cu_name[0].replace(' ', '') + t[0][-8:-2] + 'hfq'
                        ))
                except Exception as e:
                    if (cu_name[0] + t[0][-8:-2] + 'hfq') in str(e):
                        print(cu_name[0] + t[0][-8:-2] + 'hfq=======')
                    else:
                        raise e
            else:
                print(t[0], cu_name)
                cur.execute(sql_rename.format(
                        t[0],
                        cu_name[0].replace('*', '').replace(' ', '') +
                        t[0][-8:-2] + 'hfq'
                        ))
        else:
            print(t[0], cu_name)
    # stock_info_a_code_name_df = ak.stock_info_a_code_name()
    # print(stock_info_a_code_name_df)
    # save = 'y'
    # if save == 'y':
    #     stock_info_a_code_name_df.to_sql(
    #         'stock_info_a_code_name', con=conn,
    #         if_exists='replace', index=False)
    # conn.close()


# history_m_table_name()


@gl_v.time_show
def history_k_day_add():
    # import akshare as ak
    import time
    conn, cur = gl_v.get_conn_cur()
    # stock_info_a_code_name_df = ak.stock_info_a_code_name()
    # save = 'y'
    # if save == 'y':
    #     stock_info_a_code_name_df.to_sql(
    #         'stock_info_a_code_name', con=conn,
    #         if_exists='replace', index=False)

    # 查询股票中文名
    sql_china_name = """select * from stock_info_a_code_name
     where code like '00%' or code like '30%' or code like '60%'"""
    cu = cur.execute(sql_china_name)
    for t in cu.fetchall():
        print(t)
        gl_v.history_k_single(t[1].replace(' ', '').replace('*', ''),
                              t[0], conn, save='y',
                              end_date='20221115')  # fq=qfq前，=hfq后复权
        time.sleep(1.75)
    conn.close()


# history_k_day_add()


# @gl_v.time_show
# def stock_financial_analysis_indicator_df():
#     import akshare as ak
#     import time
#     conn, cur = gl_v.get_conn_cur()

#     # 查询股票中文名
#     sql_china_name = """select * from stock_info_a_code_name
#      where code like '00%' or code like '30%' or code like '60%'"""
#     cu = cur.execute(sql_china_name)
#     # for t in cu.fetchmany():
#     for t in cu.fetchall():
#         print(t[0])
#         st = ak.stock_financial_analysis_indicator(symbol=t[0])
#         # print(st)
#         time.sleep(1.65)
#         save = 'y'
#         if save == 'y':
#             st.to_sql(
#                 t[1].replace(' ', '').replace('*', '') + t[0] +
#                 'financial_indicator',
#                 con=conn, if_exists='replace', index=False)
#     conn.close()


# stock_financial_analysis_indicator_df()


@gl_v.time_show  # 业绩报表,年
def stock_yjbb_em():
    import akshare as ak
    import time
    conn, cur = gl_v.get_conn_cur()
    # arr = [
    #     '20211231',
    #     '20201231',
    #     '20191231',
    #     '20181231',
    #     '20171231',
    #     '20161231',
    #     '20151231',
    #     '20141231',
    #     '20131231',
    #     '20121231',
    #     '20111231','20101231'
    # ]
    arr = [
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
    for t in arr:
        print(t)
        st = ak.stock_yjbb_em(date=t)
        # print(st)
        time.sleep(1)
        save = ''
        if save == 'y':
            st.to_sql('stock_yjbb_em' + t, con=conn,
                      if_exists='replace', index=False)
    conn.close()


# stock_yjbb_em()


@gl_v.time_show
def stock_yjbb_em_20():
    import pandas as pd
    conn, cur = gl_v.get_conn_cur()
    # 查询股票中文名
    sql_jzcsyl = """select 股票代码, 股票简称, 净资产收益率
     from {} where 净资产收益率>{} and
    (股票代码 like '00%' or 股票代码 like '30%' or 股票代码 like '60%')"""
    # 查询申购日期
    sql_sg_day = """select 股票代码, 股票简称, 申购日期
     from stock_xgsglb_em20100113 where 股票代码='{}'"""
    arr = [
        'stock_yjbb_em20211231',
        'stock_yjbb_em20201231',
        'stock_yjbb_em20191231'
    ]
    # 获取每一季度符合条件的股票>19,非新股
    df_new20211231 = new_stock_yjbb_em_20(pd, conn, sql_sg_day, sql_jzcsyl,
                                          arr[0], x_day='2019-01-01')
    print(df_new20211231)
    df_new20201231 = new_stock_yjbb_em_20(pd, conn, sql_sg_day, sql_jzcsyl,
                                          arr[1], x_day='2018-01-01')
    print(df_new20201231)
    df_new20191231 = new_stock_yjbb_em_20(pd, conn, sql_sg_day, sql_jzcsyl,
                                          arr[2], x_day='2017-01-01')
    print(df_new20191231)
    # 循环合并21-20两df中的重复部分
    df_new_concat = new_stock_yjbb_em_20_concat(pd, df_new20211231,
                                                df_new20201231['股票代码'].values)
    print(df_new_concat)
    # 循环合并20-19两df中的重复部分
    concat_19 = new_stock_yjbb_em_20_concat(pd, df_new_concat,
                                            df_new20191231['股票代码'].values)
    print(concat_19)
    conn.close()


# 循环合并两df中的重复部分
def new_stock_yjbb_em_20_concat(pd, df_new202112, df_new202012):
    df_new_concat = pd.DataFrame()  # 循环合并两df中的重复部分
    for i, t in df_new202112.iterrows():
        # print(t['股票代码'])
        if t['股票代码'] in df_new202012:
            df_new_concat = pd.concat([df_new_concat, t], axis=1)
    df_new_concat = pd.DataFrame(df_new_concat.values.T,
                                 index=df_new_concat.columns,
                                 columns=df_new_concat.index)
    return df_new_concat


# 获取每一季度符合条件的股票>19,非新股
def new_stock_yjbb_em_20(pd, conn, sql_sg_day, sql_jzcsyl, arr_quater, x_day):
    dat = pd.read_sql(sql_jzcsyl.format(arr_quater, 19), conn)
    df_new20211231 = pd.DataFrame()  # 循环2021获取非新股
    for i, t in dat.iterrows():
        # print(t['股票代码'])
        dat_day = pd.read_sql(sql_sg_day.format(t['股票代码']), conn)
        # print(dat_day)
        if dat_day.shape[0] > 0:
            dat_day = dat_day['申购日期'].values
            # sg_day = pd.Timestamp(dat_day[0])
            # sg_day = pd.Timestamp('2019-01-01')
            if pd.Timestamp(dat_day[0]) < pd.Timestamp(x_day):
                # print(dat_day[0])
                df_new20211231 = pd.concat([df_new20211231, t], axis=1)
        else:
            df_new20211231 = pd.concat([df_new20211231, t], axis=1)
    df_new20211231 = pd.DataFrame(df_new20211231.values.T,
                                  index=df_new20211231.columns,
                                  columns=df_new20211231.index)
    return df_new20211231


# stock_yjbb_em_20()


@gl_v.time_show  # 新股申购日期
def stock_xgsglb_em():
    import akshare as ak
    conn, cur = gl_v.get_conn_cur()
    st = ak.stock_xgsglb_em(symbol="全部股票")
    print(st)
    save = ''
    if save == 'y':
        st.to_sql('stock_xgsglb_em20100113', con=conn,
                  if_exists='replace', index=False)

    conn.close()


# stock_xgsglb_em()
