# import tool_db
# import tools
# import tool_east
from . import tool_db, tools, tool_east
import pandas as pd
import akshare as ak


def stock_yjbb_em_20(quarter, len):  # 查询股票 净资产收益率
    conn, cur = tool_db.get_conn_cur()
    # 查询股票 净资产收益率
    sql_jzcsyl = """select 股票代码, 股票简称,营业收入, 营收同比, 营收季度环比,
        净利润, 净利同比, 净利季度环比, 销售毛利率 as 毛利率, 所处行业, 净资产收益率
        from {} where 净资产收益率>{} and
        (股票代码 like '00%' or 股票代码 like '30%' or 股票代码 like '60%')"""
    # 查询申购日期
    sql_sg_day = """select 股票代码, 股票简称, 申购日期
     from stock_xgsglb_em20100113 where 股票代码='{}'"""
    # 总资产收益率
    sql_zzcsyl = """select 股票代码,总资产收益率 from {} where 总资产收益率>{} and
    (股票代码 like '00%' or 股票代码 like '30%' or 股票代码 like '60%')"""
    quar_new = pd.DataFrame()
    # print(not quar_new.empty)
    for quar in quarter:
        if not quar_new.empty:
            # 获取每一季度符合条件的股票>19,非新股
            quar_new1 = new_stock_yjbb_em_20(
                conn, sql_sg_day, sql_jzcsyl, quar)
            # 循环合并两df中的重复部分
            quar_ = tools.new_stock_yjbb_em_20_delete(
                quar_new, quar_new1['股票代码'].values)

            # 总资产收益率>9
            dat_zzcsyl = pd.read_sql(
                sql_zzcsyl.format('my' + quar, 0.09), conn)
            # add column,循环合并两df中的重复部分
            quar_new = tools.add_column_concat_delete(
                quar_, dat_zzcsyl)
        else:
            # 净资产收益率>19
            quar_ = new_stock_yjbb_em_20(
                conn, sql_sg_day, sql_jzcsyl, quar)
            # 总资产收益率>9
            dat_zzcsyl = pd.read_sql(
                sql_zzcsyl.format('my' + quar, 0.09), conn)
            # add column,循环合并两df中的重复部分
            quar_new = tools.add_column_concat_delete(
                quar_, dat_zzcsyl)
            # print(quar_new)
    # 查询全部交易股票
    sql_all = """select code from stock_info_a_code_name"""
    dat_all = pd.read_sql(sql_all, conn)
    # 循环合并两df中的重复部分
    df_new_concat = tools.new_stock_yjbb_em_20_delete(
        quar_new, dat_all['code'].values)
    # print(df_new_concat)
    conn.close()
    return df_new_concat


# 获取每一季度符合条件的股票>19,非新股,上市2year以上
def new_stock_yjbb_em_20(
        conn, sql_sg_day, sql_jzcsyl, quater):
    dat = pd.read_sql(sql_jzcsyl.format('stock_yjbb_em' + quater, 19), conn)
    df_new2021 = dat.copy()
    # df_new20211231 = pd.DataFrame()
    for i, t in dat.iterrows():
        # print(t['股票代码'])
        dat_day = pd.read_sql(sql_sg_day.format(t['股票代码']), conn)
        # print(dat_day)
        if dat_day.shape[0] > 0:  # 有申购日期
            dat_day = dat_day['申购日期'].values
            x_day = str(int(quater[:4])-2) + quater[4:]
            #  申购日期大于指定日期-删除
            if pd.Timestamp(dat_day[0]) > pd.Timestamp(x_day):
                df_new2021.drop(index=[i], inplace=True)
    return df_new2021


def ak_zhang_ting(day2):  # 涨停,技术股
    day2 = day2.replace('/', '')
    print(day2)
    conn, cur = tool_db.get_conn_cur()
    # 查询有没有这个表
    sql_tab_name = """select name from sqlite_master where type='table' and
    name = '{}'"""
    # day2 = '20221118'
    dat = pd.read_sql(sql_tab_name.format('zhangTing' + day2), conn)
    if dat.shape[0] == 0:  # 如果表名不存在-进
        print('表名不存在--下载', dat)
        sto = ak.stock_zt_pool_em(date=day2)
        # print(sto)
        if not sto.empty:
            sto.to_sql('zhangTing' + day2, con=conn,
                       if_exists='replace', index=False)
        else:
            print('非交易日?')
    # breakpoint()
    # 查询某天涨停股
    da = pd.read_sql("""select * from {} where
        代码 like '00%' or 代码 like '30%' or 代码 like '60%'
        """.format('zhangTing' + day2), conn)
    conn.close()
    return da


# 实时行情转入不复权数据表
def stock_zh_a_spot_em_to_bfq(save, day):
    conn, cur = tool_db.get_conn_cur()
    # 查询股票中文名
    sql_china_name = """select 代码,名称,今开 as 开盘,最新价 as 收盘,最高,最低,
    成交量,成交额,振幅,涨跌幅,涨跌额,换手率 from '{}'
    where 代码 like '00%' or 代码 like '30%' or 代码 like '60%'"""
    dat = pd.read_sql(sql_china_name.format(
        'stock_zh_a_spot_em' + day.replace('/', '')), conn)
    # print(col)
    if dat.shape[0] > 0:
        for i, t in dat.iloc[0:].iterrows():
            # print(i, t['名称'].replace(' ', '').replace('*', ''), t['代码'])
            tt = pd.DataFrame(t.iloc[2:]).T
            tt.insert(0, '日期', day.replace('/', '-'))
            # print(tt)
            if save == 'y':
                tt.to_sql(
                    t['名称'].replace(' ', '').replace('*', '') + t['代码'],
                    con=conn,
                    if_exists='append',
                    index=False)
    else:
        print('日期有误,没有k数据2')
    conn.close


# 目标地址: http://quote.eastmoney.com/center/gridlist.html#hs_a_board
# 东方财富网-沪深京 A 股-实时行情数据
def stock_zh_a_spot_em(save, day):
    conn, cur = tool_db.get_conn_cur()
    sql_price = """select * from '{}' where 日期='{}'"""
    data2 = pd.read_sql(sql_price.format(
        '平安银行000001', day.replace('/', '-')), conn)
    data3 = pd.read_sql(sql_price.format(
        '万科A000002', day.replace('/', '-')), conn)
    if data2.shape[0] > 0 or data3.shape[0] > 0:
        print(day + '已经有数据')
    else:
        st = ak.stock_zh_a_spot_em()
        # print(st)
        if st.shape[0] > 0:
            if save == 'y':
                st.to_sql(
                    'stock_zh_a_spot_em' + day.replace('/', ''),
                    con=conn, if_exists='replace', index=False)
            # 实时行情转入不复权数据表
            stock_zh_a_spot_em_to_bfq(save, day)
        else:
            print('日期有误,没有k数据1')
    conn.close()


# 更新交易股票数据
def stock_info_a_code_name_df(conn):
    stock_info_a_code_name_df = ak.stock_info_a_code_name()
    stock_info_a_code_name_df.to_sql(
        'stock_info_a_code_name', con=conn,
        if_exists='replace', index=False)


# 中报,年报分红配送
def stock_fhps_em(save, qua, conn):
    import time
    for i in tools.get_quarter_array()[:qua]:
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


# 合并季度年度分红送股表
def concat_fhsg(save, conn):
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
    # conn.close()


# 经典计算个股复权
def fq_factor3(name2, code, fq, save, conn):
    # 查询是否有分红送股
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
        # print(d)
    if save == 'y':
        d.to_sql(name2 + code + fq, con=conn,
                 if_exists='replace', index=False)


# 计算后复权数据表
def hfq_calu_total(fq2, flat):
    # flat = ''
    conn, cur = tool_db.get_conn_cur()
    if flat:
        stock_info_a_code_name_df(conn)  # 更新交易股票数据
        # not stock_em_pg()  # 更新配股数据
        tool_east.east_history_peigu_data(save='y', conn=conn)
        # 更新中报,年报分红配送(最近2季度)
        stock_fhps_em(save='y', qua=2, conn=conn)
        # 合并季度年度分红送股表
        concat_fhsg(save='y', conn=conn)
    # 查询股票中文名
    sql_china_name = """select * from stock_info_a_code_name
    where code like '00%' or code like '30%' or code like '60%'"""
    dat = pd.read_sql(sql_china_name, conn)
    # print(dat)
    for i, t in dat.iloc[0:].iterrows():
        print(i, t['name'].replace(' ', '').replace('*', ''), t['code'])
        # 经典计算复权
        fq_factor3(t['name'].replace(' ', '').replace('*', ''), t['code'],
                   fq2, save='y', conn=conn)
    conn.close


def history_k_single(name2, code2, conn='', save='',
                     end_date='', fq=''):  # 获取数据并保存数据库
    """
    目标地址: http://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)
    描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新
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
            conn.close()
        else:
            stock_zh_a_hist_df.to_sql(
                name2+code2+fq, con=conn, if_exists='replace', index=False)


# 获取年报季度报三大报表
class AkYearQuaterTable(object):
    def __init__(self):
        pass

    @tools.time_show  # east资产负债表,年
    def stock_zcfz_em(self):
        # import tool_db
        # import tools
        # import tool_east
        """
        19990930 'NoneType' object is not subscriptable=9
        19970930
        19940930
        19920930
        19920331
        19910930
        19910331
        19900930
        19900331
        """
        conn = tool_db.get_conn_cur()
        arr = tools.get_quarter_array()
        # 正常只需要更新最新前２个季度数据
        for t in arr[0:2]:
            try:
                st = ak.stock_zcfz_em(date=t)
                st.rename(columns={
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
                # print(st)
                print(t)
                st.to_sql('stock_zcfz_em' + t, con=conn,
                          if_exists='replace', index=False)
                # time.sleep(0.5)
            except TypeError as e:
                print(t, e)
        conn.close()

    @tools.time_show  # east利润表,年
    def stock_lrb_em(self):
        # import akshare as ak
        # import time
        conn = tool_db.get_conn_cur()
        arr = tools.get_quarter_array()
        # 正常只需要更新最新前２个季度数据
        for t in arr[0:2]:
            try:
                st = ak.stock_lrb_em(date=t)
                st.rename(columns={
                    '营业总支出-营业支出': '营业支出',
                    '营业总支出-销售费用': '销售费用',
                    '营业总支出-管理费用': '管理费用',
                    '营业总支出-财务费用': '财务费用',
                    '营业总支出-营业总支出': '营业总支出',
                }, inplace=True)
                print(t)
                # print(st)
                st.to_sql('stock_lrb_em' + t, con=conn,
                          if_exists='replace', index=False)
                # time.sleep(0.9)
            except TypeError as e:
                print(t, e)
        conn.close()


# sto = AkYearQuaterTable()
# sto.stock_zcfz_em()  #  east资产负债表
# sto.stock_lrb_em()  # east利润表
