from . import tool_db
import pandas as pd


def stock_yjbb_em_20():
    conn, cur = tool_db.get_conn_cur()
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
