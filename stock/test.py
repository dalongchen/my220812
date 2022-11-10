import sqlite3
import global_variable as gl_v

@gl_v.time_show
def history_volatility(db=gl_v.db_path_poll,sql="select 日期,涨跌幅 from 'df601398hfq'"):
    #连接数据库，如果不存在，则自动创建
    conn=sqlite3.connect(db)
    #创建游标
    cur=conn.cursor()
    cu=cur.execute(sql)
    print(cu.fetchone())
    #关闭游标
    cur.close()
    #断开数据库连接
    conn.close()
    return cu
       
# history_volatility(gl_v.db_path_poll)

@gl_v.time_show
def history_test(last_day, save=''):
    import akshare as ak
    import time

    # stock_zh_a_daily_hfq_df = ak.stock_zh_a_daily(symbol="sz000002", start_date='20221101',
    # end_date='20221105',  adjust="hfq")
    # print(stock_zh_a_daily_hfq_df)  801780

    # stock_sector_detail_df = ak.stock_board_industry_name_ths()
    # print(stock_sector_detail_df)

    # 获取银行成分股
    stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(symbol="银行")
    print(stock_board_industry_cons_ths_df[["代码","名称"]])
    conn=sqlite3.connect(gl_v.db_path_poll)
    cur=conn.cursor()
    for a in stock_board_industry_cons_ths_df[["代码","名称"]].values:
        print(a[0],a[1])
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=a[0], period="daily", start_date='', 
        end_date=last_day,adjust="hfq")
        # print(stock_zh_a_hist_df)
        if save == 'y':
            stock_zh_a_hist_df.to_sql(a[1]+a[0]+'hfq', con=conn, if_exists='replace', index=False)
        time.sleep(1.23)
    # 查询有没有这个表
    # sql_tab_name= "select name from sqlite_master where type='table' and name like '%{}%'";
    # 查询该表的最新一条数据
    # sql_new= "select * from '{}' ORDER BY date desc limit 1";
    # 更新该表的最新一条数据
    # sql_update= "update '{}' set state='0' where date='{}'";
    # for a in stock_board_industry_cons_ths_df["代码"].values:
    #     # print(a+'_2')
    #     cu=cur.execute(sql_tab_name.format(a+'_2')).fetchone() # 查询有没有这个表
    #     print(cu==None)
    #     if cu==None:
    #         print()
    #     else:
    #         cu2=cur.execute(sql_new.format(cu[0])).fetchone()  # 查询该表的最新一条数据
    #         print(cu2)
    #         if cu2[0]==last_day:  # 最后一条数据和现在日期一致->查询east该日期数据进行比较
    #             # print(a,cu2[0].replace('-', ''),cu2)
    #             cu2_=cu2[0].replace('-', '')
    #             # 查询east该日期数据
    #             stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=a, period="daily", start_date=cu2_, 
    #             end_date=cu2_,adjust="hfq")
    #             print(stock_zh_a_hist_df)
    #             if stock_zh_a_hist_df['收盘'].values!=cu2[2]: # 收盘价如果不同-更新该日期数据
    #                 # cu2=cur.execute(sql_new.format(cu[0]))  # 更新该表的最新一条数据
    #                 print('oup')
    #         else:
    #             print()
    
    cur.close()
    conn.close()
       
# history_test(last_day='20221105',save='y')


@gl_v.time_show
def history_divident_test(save =''):  # 下载股票分红
    import akshare as ak
    import time

    # 获取银行成分股
    stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(symbol="银行")
    print(stock_board_industry_cons_ths_df[["代码","名称"]])
    conn=sqlite3.connect(gl_v.db_path_poll)
    cur=conn.cursor()
    for a in stock_board_industry_cons_ths_df[["代码","名称"]].values:
        print(a[0],a[1])
        stock_dividents_cninfo_df = ak.stock_dividents_cninfo(symbol=a[0])
        # print(stock_dividents_cninfo_df)
        if save == 'y':
            stock_dividents_cninfo_df.to_sql(a[1]+a[0]+'分红', con=conn, if_exists='replace', index=False)
        time.sleep(1.23)
    cur.close()
    conn.close()
# history_divident_test(save ='y')

@gl_v.time_show
def history_divident_test_local():  # 分析股票分红
    import pandas as pd
    conn=sqlite3.connect(gl_v.db_path_poll)
    # cur=conn.cursor()
    li=[
        '交通银行601328分红',
        '中国银行601988分红',
        '建设银行601939分红',
        '工商银行601398分红',
        '农业银行601288分红'
    ]
    sql = r"""select * from {}"""
    for a in li:
        dat2 = pd.read_sql(sql.format(a), conn )
        print(a,dat2['派息比例'].sum())
    # cur.close()
    conn.close()
    
history_divident_test_local()

@gl_v.time_show
def stock_a_lg_indicator(save=''):  #  下载市盈市净股息
    import akshare as ak
    import time

    # 获取银行成分股
    stock_board_industry_cons_ths_df = ak.stock_board_industry_cons_ths(symbol="银行")
    print(stock_board_industry_cons_ths_df[["代码","名称"]])
    conn=sqlite3.connect(gl_v.db_path_poll)
    cur=conn.cursor()
    for a in stock_board_industry_cons_ths_df[["代码","名称"]].values:
        print(a[0],a[1])
        stock_a_lg_indicator_df = ak.stock_a_lg_indicator(symbol=a[0])
        # print(stock_a_lg_indicator_df)
        if save == 'y':
            stock_a_lg_indicator_df.to_sql(a[1]+a[0]+'市盈市净股息', con=conn, if_exists='replace', index=False)
        time.sleep(1.23)
    cur.close()
    conn.close()
# stock_a_lg_indicator(save='y')

@gl_v.time_show
def stock_a_lg_indicator_local():  # 分析市盈市净股息
    import pandas as pd
    conn=sqlite3.connect(gl_v.db_path_poll)
    # cur=conn.cursor()
    li=[
        '交通银行601328市盈市净股息',
        '中国银行601988市盈市净股息',
        '建设银行601939市盈市净股息',
        '工商银行601398市盈市净股息',
        '农业银行601288市盈市净股息'
    ]
    # sql = r"""select * from {}"""
    sql = r"""select dv_ratio from {} ORDER BY trade_date desc limit 244*4"""
    for a in li:
        dat2 = pd.read_sql(sql.format(a), conn )
        print(a,dat2.mean())
    # cur.close()
    conn.close()
    
# stock_a_lg_indicator_local()