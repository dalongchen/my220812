import sqlite3
import global_variable as gl_v
import pandas as pd


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
    conn, cur = gl_v.get_conn_cur()
    if f == 'one':
        gl_v.history_k_single(name2, code2, conn, save, end_date)

    if f == 'mult':
        import time
        import pandas as pd
        # import akshare as ak
        # # 更新交易股票数据
        # stock_info_a_code_name_df = ak.stock_info_a_code_name()
        # save = 'y'
        # if save == 'y':
        #     stock_info_a_code_name_df.to_sql(
        #         'stock_info_a_code_name', con=conn,
        #         if_exists='replace', index=False)

        # 查询股票中文名
        sql_china_name = """select * from stock_info_a_code_name
        where code like '00%' or code like '30%' or code like '60%'"""
        dat = pd.read_sql(sql_china_name, conn)
        # print(dat)
        for i, t in dat.iloc[4:].iterrows():
            print(t)
            gl_v.history_k_single(t[1].replace(' ', '').replace('*', ''),
                                  t[0], conn, save='y',
                                  end_date=end_date)
            time.sleep(0.2)
    conn.close()


# md = ['美的集团', '000333']
# tqly = ['天齐锂业', '002466']
# tqly = ['浦发银行', '600000']
# history_k_day_add(f='one', name2=tqly[0], code2=tqly[1], save='y',
#                   end_date='20221118')  # f='one'-单个股票
# history_k_day_add(f='mult', save='y', end_date='20221125')  # f='mult'-mor个股票


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
def stock_fhps_em(save, qua):
    import akshare as ak
    import time
    conn, cur = gl_v.get_conn_cur()
    for i in gl_v.get_quarter_array(f='year_middle')[:qua]:
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
        time.sleep(0.4)
    conn.close()


# stock_fhps_em()


@gl_v.time_show  # 地址: http://data.eastmoney.com/xg/pg/ 配股
def stock_em_pg(save='y'):
    import akshare as ak
    conn, cur = gl_v.get_conn_cur()
    # print('stttt')
    st = ak.stock_em_pg()
    print(st)
    if save == 'y':
        st.to_sql("stock_em_pg", con=conn,
                  if_exists='replace', index=False)
    conn.close()


# stock_em_pg()


# @gl_v.time_show  # 经典计算个股复权
def fq_factor3(name2, code, fq, save, conn=''):
    # print(not conn)
    if not conn:
        conn, cur = gl_v.get_conn_cur()
    # 查询是否有分红送股
    # sql_s = r"""select 送转总比例,现金分红比例,除权除息日 as 除权日 from {} where 代码='{}'
    # and 方案进度='实施分配' and 除权除息日!=Null"""
    sql_s = r"""select 送转总比例,现金分红比例,除权除息日 as 除权日 from {} where 代码='{}'
    and 方案进度='实施分配' and 除权除息日!='Null'"""
    dat = pd.read_sql(sql_s.format('fhsg_total', code), conn).fillna(0)
    # print(dat)
    # 查询是否有配股
    sql_peigu = r"""select 配股比,配股价,除权日 from {} where 代码='{}'"""
    da = pd.read_sql(sql_peigu.format('east_history_peigu', code), conn)
    # 获取分红送股和配股中日期相同的数据
    result = pd.merge(dat, da, on='除权日')
    k = result.pop("除权日")
    result.insert(result.shape[1], "除权日", k)  # 将除权日列移到了最后一列去
    # print(result)
    dat.insert(2, '配股比', 0)
    dat.insert(3, '配股价', 0)
    if da.shape[0] > 0:  # 有配股数据就合并
        da.insert(0, '送转总比例', 0)
        da.insert(1, '现金分红比例', 0)
        # 合并分红送股和配股中日期相同的数据
        if result.shape[0] > 0:
            for i, ss in result.iterrows():
                # print(ss.values)
                con_day = dat.loc[
                    dat['除权日'] == ss['除权日']
                ]
                con_day2 = da.loc[
                    da['除权日'] == ss['除权日']
                ]
                dat.loc[con_day.index] = ss.values
                da = da.drop(labels=con_day2.index)
                # print(dat.loc[con_day.index])
        dat = pd.concat([dat, da], axis=0)  # 纵向合并
    if code == '000038':
        con_ = dat.loc[
                        dat['除权日'] == 0
                    ]
        dat.loc[con_.index] = [3, 0, 0, 0, '2009-05-04']
        # print(con_)
    dat = dat.sort_values(by="除权日", ignore_index=True)  # 排序
    # print(dat)
    # 查询除权时收盘价...
    sql_price = r"""select * from {}"""
    d = pd.read_sql(sql_price.format(name2 + code), conn)
    if dat.shape[0] > 0:
        ps_total = 1  # 送股总比
        pg_total = 1  # 配股总比
        pg_total_amount = 0  # 配股总金额
        fh_total = 0  # 总分红
        # ss2 = pd.DataFrame()
        for i, s in dat.iloc[0:].iterrows():
            # print(s, '--------')
            # if not ss2.empty:
            #     s = ss2
            #     print(s, '=======')
            cqr = s['除权日']
            if i < (dat.shape[0]-1):
                hcqr = dat.loc[i+1, '除权日']
                # 查询收盘价
                day_ = d.loc[d['日期'] >= cqr]
                day_ = day_.loc[day_['日期'] < hcqr]
            else:
                day_ = d.loc[d['日期'] >= cqr]
            # if day_.shape[0] < 2:
            #     if (s['配股比'] and s['配股价'] and
            #        (not s['现金分红比例']) and (not s['送转总比例'])):
            #         dat.loc[i+1, '配股比'] = s['配股比']
            #         dat.loc[i+1, '配股价'] = s['配股价']
            #         ss2 = dat.loc[i+1]
            #         # print(ss2)
            #     continue
            fh_total += (s['现金分红比例']/10)*ps_total*pg_total
            # print(ps_total, fh_total)
            ps_total *= (1+s['送转总比例']/10)
            pg_total_amount += pg_total*ps_total*(s['配股比']/10)*s['配股价']
            pg_total *= (1+s['配股比']/10)
            open_ = (
                day_['开盘']*ps_total*pg_total +
                fh_total -
                pg_total_amount
            )
            close_ = (
                day_['收盘']*ps_total*pg_total +
                fh_total -
                pg_total_amount
            )
            high_ = (
                day_['最高']*ps_total*pg_total +
                fh_total -
                pg_total_amount
            )
            low_ = (
                day_['最低']*ps_total*pg_total +
                fh_total -
                pg_total_amount
            )
            # print(close_)
            d.loc[day_.index, ['开盘']] = open_
            d.loc[day_.index, ['收盘']] = close_
            d.loc[day_.index, ['最高']] = high_
            d.loc[day_.index, ['最低']] = low_
            # print(d.loc[day_.index])
            # ss2 = pd.DataFrame()  # ss2置空,避免前面条件一直成立
        # print(d)
    if save == 'y':
        # print(d)
        d.to_sql(name2 + code + fq, con=conn,
                 if_exists='replace', index=False)
        # conn.commit()
    if not conn:
        conn.close()


# tqly = ['万科A', '000002']
# tqly = ['浦发银行', '600000']
# tqly = ['天齐锂业', '002466']
# fq_factor3(tqly[0] + tqly[1], tqly[1], save='')
# fq_factor3('美的集团000333', '000333', save='')


@gl_v.time_show  # 合并季度年度分红送股表
def concat_fhsg(save=''):
    conn, cur = gl_v.get_conn_cur()
    # 查询有没有表
    sql_tab_name = """select name from sqlite_master where type='table' and
    name like '{}%'"""
    dat = pd.read_sql(sql_tab_name.format('stock_fhps_em'), conn)
    d_fhsg = pd.DataFrame()
    for i, t in dat.iterrows():
        da = pd.read_sql("""select * from {}""".format(t['name']), conn)
        # print(t['name'])
        d_fhsg = pd.concat([d_fhsg, da], axis=0)  # 纵向合并
    # print(d_fhsg)
    if save == 'y':
        d_fhsg.to_sql('fhsg_total', con=conn,
                      if_exists='replace', index=False)
    conn.close()


# concat_fhsg(save='y')


# 更新交易股票数据
def stock_info_a_code_name_df(conn):
    import akshare as ak
    stock_info_a_code_name_df = ak.stock_info_a_code_name()
    save = 'y'
    if save == 'y':
        stock_info_a_code_name_df.to_sql(
            'stock_info_a_code_name', con=conn,
            if_exists='replace', index=False)


@gl_v.time_show  # 实时行情转入不复权数据表
def stock_zh_a_spot_em_to_bfq(save, day):
    conn, cur = gl_v.get_conn_cur()
    # 查询股票中文名
    sql_china_name = """select 代码,名称,今开 as 开盘,最新价 as 收盘,最高,最低,
    成交量,成交额,振幅,涨跌幅,涨跌额,换手率 from '{}'
    where 代码 like '00%' or 代码 like '30%' or 代码 like '60%'"""
    dat = pd.read_sql(sql_china_name.format(
        'stock_zh_a_spot_em' + day.replace('-', '')), conn)
    # print(col)
    for i, t in dat.iloc[0:].iterrows():
        print(i, t['名称'].replace(' ', '').replace('*', ''), t['代码'])
        tt = pd.DataFrame(t.iloc[2:]).T
        tt.insert(0, '日期', day)
        # print(tt)
        if save == 'y':
            tt.to_sql(
                t['名称'].replace(' ', '').replace('*', '') + t['代码'],
                con=conn,
                if_exists='append',
                index=False)
    conn.close


# stock_zh_a_spot_em_to_bfq(save='y', day='2022-11-28')


# 目标地址: http://quote.eastmoney.com/center/gridlist.html#hs_a_board
# 东方财富网-沪深京 A 股-实时行情数据
def stock_zh_a_spot_em(save, day):
    import akshare as ak
    conn, cur = gl_v.get_conn_cur()
    st = ak.stock_zh_a_spot_em()
    print(st)
    if save == 'y':
        st.to_sql('stock_zh_a_spot_em' + day, con=conn,
                  if_exists='replace', index=False)


# stock_zh_a_spot_em(save='y', day='20221128')


@gl_v.time_show  # 计算后复权数据表
def hfq_calu_total(fq, flat):
    import test_east
    conn, cur = gl_v.get_conn_cur()
    if flat:
        stock_info_a_code_name_df(conn)  # 更新交易股票数据
        # not stock_em_pg()  # 更新配股数据
        test_east.east_history_peigu_data(save='y')
        # 更新中报,年报分红配送(最近2季度)
        stock_fhps_em(save='y', qua=2)
        # 合并季度年度分红送股表
        concat_fhsg(save='y')
    # 查询股票中文名
    sql_china_name = """select * from stock_info_a_code_name
    where code like '00%' or code like '30%' or code like '60%'"""
    dat = pd.read_sql(sql_china_name, conn)
    # print(dat)
    for i, t in dat.iloc[0:].iterrows():
        print(i, t['name'].replace(' ', '').replace('*', ''), t['code'])
        # if t['code'] == '605168':
        #     break
        # 经典计算复权
        fq_factor3(t['name'].replace(' ', '').replace('*', ''), t['code'],
                   fq, save='y', conn=conn)
    conn.close


# hfq_calu_total(fq='hfq', flat='')


@gl_v.time_show  # 去重
def qc_replace():
    conn, cur = gl_v.get_conn_cur()
    # 查询股票中文名
    sql_china_name = """select * from stock_info_a_code_name
    where code like '00%' or code like '30%' or code like '60%'"""
    dat = pd.read_sql(sql_china_name, conn)
    # print(dat)
    sql_name = """select * from '{}'"""
    for i, t in dat.iloc[0:].iterrows():
        tab = t['name'].replace(' ', '').replace('*', '') + t['code']
        print(i, tab)
        da = pd.read_sql(sql_name.format(tab), conn)
        da = da.iloc[0:-4]
        # print(da)
        da.to_sql(tab, con=conn,
                  if_exists='replace', index=False)
    conn.close


qc_replace()
