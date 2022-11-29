from . import tool_db, tools
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
        save = 'y'
        if (save == 'y') and not sto.empty:
            sto.to_sql('zhangTing' + day2, con=conn,
                       if_exists='replace', index=False)
    # breakpoint()
    # 查询某天涨停股
    da = pd.read_sql("""select * from {} where
        代码 like '00%' or 代码 like '30%' or 代码 like '60%'
        """.format('zhangTing' + day2), conn)
    conn.close()
    return da


# 目标地址: http://quote.eastmoney.com/center/gridlist.html#hs_a_board
# 东方财富网-沪深京 A 股-实时行情数据
def stock_zh_a_spot_em(save, day):
    conn, cur = tool_db.get_conn_cur()
    st = ak.stock_zh_a_spot_em()
    # print(st)
    if st.shape[0] > 0:
        if save == 'y':
            st.to_sql('stock_zh_a_spot_em' + day.replace('/', ''), con=conn,
                      if_exists='replace', index=False)
    else:
        print('日期有误,没有k数据1')
    conn.close()


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

# def ak_update_day_k(day2):  # 更新history day k线数据和在交易股票表
    # import datetime
    # import time
    # day2 = day2.replace('/', '')
    # # print(day2)
    # conn, cur = tool_db.get_conn_cur()
    # #  保存在交易股票表
    # save2 = 'y'
    # if save2 == 'y':
    #     ak.stock_info_a_code_name().to_sql(
    #         'stock_info_a_code_name', con=conn,
    #         if_exists='replace', index=False)
    # # 查询在交易股票
    # sql_tab_name = r"""select * from stock_info_a_code_name where
    #     code like '00%' or code like '30%' or code like '60%'"""
    # dat = pd.read_sql(sql_tab_name, conn)
    # #  查询日k数据的最后日期
    # day_new = pd.read_sql(
    #     r"""select 日期 from {} order by 日期 desc limit 1""".format(
    #         dat.iloc[0]['name'].replace(' ', '').replace('*', '') +
    #         dat.iloc[0]['code'] + 'hfq'), conn)
    # day_new = pd.to_datetime(day_new['日期']) + datetime.timedelta(1)
    # # print(day_new)  # 日期加1天
    # day_new = str(day_new.values[0]
    #               ).replace('T00:00:00.000000000', '').replace('-', '')
    # print(day_new)
    # for i, t in dat.iloc[2:].iterrows():
    #     print(t['name'].replace(' ', '').replace('*', '') + t['code'])
    #     history_k_add(t['name'].replace(' ', '').replace('*', ''),
    #                   t['code'], day_new, day2, conn=conn,
    #                   save='y', fq='hfq')  # 获取数据并保存数据库
    #     time.sleep(0.5)
    # conn.close()
    # return da


# def history_k_add(name2, code2, start_date, end_date, conn='', save='',
#                   fq='hfq'):  # 获取数据并保存数据库
#     # 获取最近几天k数据
#     st = ak.stock_zh_a_hist(
#         symbol=code2, period="daily", start_date=start_date,
#         end_date=end_date, adjust=fq)
#     print(st)
#     # breakpoint()
#     if save == 'y':
#         if conn == '':
#             from . import tool_db
#             conn, cur = tool_db.get_conn_cur()
#             st.to_sql(name2 + code2 + fq, con=conn,
#                       if_exists='append', index=False)
#             conn.commit()
#             conn.close()
#         else:
#             print('save', name2)
#             st.to_sql(name2 + code2 + fq, con=conn,
#                       if_exists='append', index=False)
