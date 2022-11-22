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
def history_k_day_add(f='', name2='', code2='', save='', end_date=''):
    """
    目标地址: http://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)
    描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取
    限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
    """
    import time
    conn, cur = gl_v.get_conn_cur()
    if f == 'one':
        gl_v.history_k_single(name2, code2, conn, save, end_date)

    if f == 'mult':
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
                                  end_date=end_date)
            time.sleep(0.75)
    conn.close()


# md = ['美的集团', '000333']
# tqly = ['天齐锂业', '002466']
# history_k_day_add(f='one', name2=tqly[0], code2=tqly[1], save='y',
#                   end_date='20221118')  # f='one'-单个股票


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


@gl_v.time_show  # east资产负债表,年
def stock_zcfz_em():
    import akshare as ak
    import time
    conn, cur = gl_v.get_conn_cur()
    """notice修改列名
        sql_zcfzb2 = "select * from {}"
        da = pd.read_sql(sql_zcfzb2.format('stock_zcfz_em' + t), conn)
        da.rename(columns={
            '资产-货币资金': '货币资金',
            '资产-应收账款': '应收账款',
            '资产-存货': '存货',
            '资产-总资产': '总资产',
            '资产-总资产同比': '总资产同比',
            '负债-应付账款': '应付账款',
            '负债-预收账款': '预收账款',
            '负债-总负债': '总负债',
            '负债-总负债同比': '总负债同比',
        }, inplace=True)
        print(da)
    """
    arr = [
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
    for t in arr[1:]:
        print(t)
        st = ak.stock_zcfz_em(date=t)
        # print(st)
        time.sleep(0.5)
        save = ''
        if save == 'y':
            st.to_sql('stock_zcfz_em' + t, con=conn,
                      if_exists='replace', index=False)
    conn.close()


# stock_zcfz_em()


@gl_v.time_show  # east利润表,年
def stock_lrb_em():
    import akshare as ak
    import time
    conn, cur = gl_v.get_conn_cur()
    """修改列名
        da = pd.read_sql(sql_lrb.format(t['name']), conn)
        da.rename(columns={
            '营业收入-营业收入': '营业收入',
            '营业收入-同比增长': '营收同比',
            '营业收入-季度环比增长': '营收季度环比',
            '净利润-净利润': '净利润',
            '净利润-同比增长': '净利同比',
            '净利润-季度环比增长': '净利季度环比',
        }, inplace=True)
    """
    arr = [
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
    for t in arr[1:]:
        print(t)
        st = ak.stock_lrb_em(date=t)
        print(st)
        save = ''
        if save == 'y':
            st.to_sql('stock_lrb_em' + t, con=conn,
                      if_exists='replace', index=False)
        time.sleep(0.9)
    conn.close()


# stock_lrb_em()


@gl_v.time_show  # 构建自己的季度净资产收益率,总资产收益率
def get_my_quarter():
    import pandas as pd
    conn, cur = gl_v.get_conn_cur()
    arr = gl_v.get_quarter_array()
    # 查询有没有这个表
    sql_jlrb = """select 股票代码,股票简称,净利润 from {}"""
    sql_zcfzb = r"""select 股票代码,股东权益合计,总资产 from {}"""
    for t in arr[1:]:
        print(t)
        dat = pd.read_sql(sql_jlrb.format('stock_lrb_em' + t), conn)
        dat2 = pd.read_sql(sql_zcfzb.format('stock_zcfz_em' + t), conn)
        dat_c = dat[['股票代码', '股票简称']].copy()
        # print(dat_c)
        zzc = []
        for i, tt in dat.iterrows():
            aa = dat2[dat2["股票代码"] == tt['股票代码']]
            if not aa.empty:
                if aa['总资产'].values:
                    zzc.append(round(tt['净利润']/aa['总资产'].values[0], 4))
                else:
                    zzc.append('')
            else:
                # print(t['股票代码'])
                zzc.append('')
                # break
        dat_c['总资产收益率'] = zzc
        # print(dat_c)
        save = ''
        if save == 'y':
            dat_c.to_sql('my' + t, con=conn,
                         if_exists='replace', index=False)
    conn.close()


# get_my_quarter()


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


@gl_v.time_show  # 删除表
def delete_table():
    import pandas as pd
    conn, cur = gl_v.get_conn_cur()
    # 查询有没有这个表
    sql_tab_name = """select name from sqlite_master where type='table' and
    name like '%{}'"""
    # sql_drop = """drop table {}"""
    dat = pd.read_sql(sql_tab_name.format('financial_indicator'), conn)
    for i, t in dat.iterrows():
        print(t['name'])
        # cur.execute(sql_drop.format(t['name']))

    conn.close()


# delete_table()


@gl_v.time_show  # 修改列名
def update_column():
    import pandas as pd
    conn, cur = gl_v.get_conn_cur()
    # 查询有没有这个表
    sql_tab_name = """select name from sqlite_master where type='table' and
    name like '{}%'"""
    sql_lrb = "select * from {}"
    dat = pd.read_sql(sql_tab_name.format('stock_yjbb_em20'), conn)
    for i, t in dat.iterrows():
        print(t['name'])
        da = pd.read_sql(sql_lrb.format(t['name']), conn)
        da.rename(columns={
            '营业收入-营业收入': '营业收入',
            '营业收入-同比增长': '营收同比',
            '营业收入-季度环比增长': '营收季度环比',
            '净利润-净利润': '净利润',
            '净利润-同比增长': '净利同比',
            '净利润-季度环比增长': '净利季度环比',
        }, inplace=True)
        # print(da)
        save = 'y'
        if save == 'y':
            da.to_sql(t['name'], con=conn, if_exists='replace', index=False)

    conn.close()


# update_column()


@gl_v.time_show  # 中报,年报分红配送
def stock_fhps_em(save=''):
    import akshare as ak
    import time
    conn, cur = gl_v.get_conn_cur()
    for i in gl_v.get_quarter_array(f='year_middle')[:1]:
        st = ak.stock_fhps_em(date=i)
        print("stock_fhps_em" + i)
        st.rename(columns={
            '送转股份-送转总比例': '送转总比例',
            '送转股份-送转比例': '送转比例',
            '送转股份-转股比例': '转股比例',
            '现金分红-现金分红比例': '现金分红比例',
            '现金分红-股息率': '股息率',
        }, inplace=True)
        print(st)
        if save == 'y':
            st.to_sql("stock_fhps_em" + i, con=conn,
                      if_exists='replace', index=False)
        time.sleep(0.7)
    conn.close()


# stock_fhps_em()


@gl_v.time_show  # 地址: http://data.eastmoney.com/xg/pg/ 配股
def stock_em_pg(save='y'):
    import akshare as ak
    conn, cur = gl_v.get_conn_cur()
    print('stttt')
    st = ak.stock_em_pg()
    print(st)
    if save == 'y':
        st.to_sql("stock_em_pg", con=conn,
                  if_exists='replace', index=False)
    conn.close()


# stock_em_pg()


@gl_v.time_show  # 计算复权因子
def fq_factor(tab, code, save=''):
    import pandas as pd
    conn, cur = gl_v.get_conn_cur()
    # 查询有没有表
    sql_tab_name = """select name from sqlite_master where type='table' and
    name like '{}%'"""
    # 查询是否有分红送股
    sql_s = r"""select 代码,名称,送转总比例,现金分红比例,除权除息日 from {} where 代码='{}'"""
    dat = pd.read_sql(sql_tab_name.format(tab), conn)
    dfr = pd.DataFrame()
    # 查询前收盘价
    sql_s = r"""select 收盘价 from {} where 日期<'{}'"""
    for i, t in dat.iterrows():
        s = pd.read_sql(sql_s.format(t['name'], code), conn)
        if s.shape[0] > 0:
            # 查询前收盘价
            d = pd.read_sql(sql_tab_name.format(tab, s['除权除息日']), conn)
            # print(s[['代码', '名称', '', '现金分红比例', '除权除息日']])
            dfr = pd.concat([dfr, s], axis=0)  # 纵向合并
    print(dfr)
    # 查询是否有配股
    sql_s = r"""select 代码,名称,配股比,除权日 from {} where 代码='{}'"""
    da = pd.read_sql(sql_tab_name.format('east_history_peigu', code), conn)
    print(da, 'klllpp')
    # if save == 'y':
    #     s.to_sql(tab, con=conn,
    #              if_exists='replace', index=False)
    conn.close()


fq_factor('stock_fhps_em', '000333', save='')
# fq_factor('美的集团000333', save='y')


