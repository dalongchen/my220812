from . import tool_db, tools
import pandas as pd


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


# def stock_yjbb_em_20(quarter, len):  # 查询股票 净资产收益率
#     conn, cur = tool_db.get_conn_cur()
#     # 查询股票 净资产收益率
#     sql_jzcsyl = """select 股票代码, 股票简称, 净资产收益率
#      from {} where 净资产收益率>{} and
#     (股票代码 like '00%' or 股票代码 like '30%' or 股票代码 like '60%')"""
#     # 查询申购日期
#     sql_sg_day = """select 股票代码, 股票简称, 申购日期
#      from stock_xgsglb_em20100113 where 股票代码='{}'"""
#     quar_new = pd.DataFrame()
#     # print(not quar_new.empty)
#     for quar in quarter:
#         if not quar_new.empty:
#             # 获取每一季度符合条件的股票>19,非新股
#             quar_new1 = new_stock_yjbb_em_20(
#                 conn, sql_sg_day, sql_jzcsyl, quar)
#             # 循环合并两df中的重复部分
#             quar_new = tools.new_stock_yjbb_em_20_delete(
#                 quar_new, quar_new1['股票代码'].values)
#         else:
#             quar_new = new_stock_yjbb_em_20(
#                 conn, sql_sg_day, sql_jzcsyl, quar)
#     # 查询全部交易股票
#     sql_all = """select code from stock_info_a_code_name"""
#     dat_all = pd.read_sql(sql_all, conn)
#     # 循环合并两df中的重复部分
#     df_new_concat = tools.new_stock_yjbb_em_20_delete(
#         quar_new, dat_all['code'].values)
#     print(df_new_concat)
#     conn.close()
#     return df_new_concat


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
