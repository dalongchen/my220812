# import numpy as np
# import pandas as pd
import sqlite3
import global_variable as gl_v
import akshare as ak


# 获取行情价
class AkStockPrice(object):
    def __init__(self):
        pass

    @staticmethod  # 新浪财务指标
    def ak_stock_finance(code, save=''):
        st_financial = ak.stock_financial_analysis_indicator(symbol=code, )
        st_financial.columns = st_financial.columns.str.split('(').str[0]
        # print(st_financial.columns)
        # stock_financial.index = stock_financial.index.strftime('%Y-%m-%d')
        if save == "y":
            with sqlite3.connect(gl_v.db_path) as conn:
                st_financial.to_sql(code + 'finance', con=conn, if_exists='replace', index=True)

    @staticmethod  # 网易获取未复权k线数据.流通市值、总市值
    def wy_stock_price(code, save='', adjust=''):
        # stock_df = ak.stock_zh_a_daily(symbol=code, adjust=adjust)
        stock_df = ak.stock_zh_a_hist_163(symbol=code)
        stock_df = stock_df.drop(labels=['股票代码', '名称'], axis=1)  # 同时删除a,b列
        # stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20210904", end_date='20210907', adjust="")
        # print(stock_df)
        # stock_df.index = stock_df.index.strftime('%Y-%m-%d')
        if save == "y":
            with sqlite3.connect(gl_v.db_path) as conn:
                stock_df.to_sql(code + adjust, con=conn, if_exists='replace', index=False)

    @staticmethod  # 东方复权k线数据
    def df_stock_price_q_hfq(code, save='', adjust=''):
        # stock_df = ak.stock_zh_a_daily(symbol=code, adjust=adjust)  # 新浪复权数据不可用
        stock_df = ak.stock_zh_a_hist(symbol=code, adjust=adjust)
        # stock_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20210904", end_date='20210907', adjust="")
        stock_df = stock_df.drop(labels=['成交量', '成交额', '涨跌额', '换手率'], axis=1)  # 同时删除a,b列
        if save == "y":
            with sqlite3.connect(gl_v.db_path) as conn:
                stock_df.to_sql('df' + code + adjust, con=conn, if_exists='replace', index=False)

    @staticmethod  # east年度利润表
    def east_finance(code, save=''):
        east_y = ak.stock_profit_sheet_by_yearly_em(symbol=code)
        # east_y = east_y.drop(labels=['SECUCODE',
        #                              'SECURITY_NAME_ABBR',
        #                              'ORG_CODE',
        #                              'ORG_TYPE',
        #                              'REPORT_TYPE',
        #                              'SECURITY_TYPE_CODE',
        #                              'CURRENCY',
        #                              'TOI_OTHER',
        #                              'TOC_OTHER',
        #                              'EFFECT_TP_OTHER',
        #                              'EFFECT_TP_OTHER_YOY',
        #                              'TOTAL_PROFIT_BALANCE',
        #                              'EFFECT_TCI_BALANCE',
        #                              'TCI_BALANCE',
        #                              'REPORT_DATE_NAME'], axis=1)  # 同时删除a,b列
        # east_y = east_y.dropna(axis=1, how='all')  # 去除空列
        east_y['REPORT_DATE'] = east_y['REPORT_DATE'].str[:10]  # 去除日期后面的0000
        east_y['NOTICE_DATE'] = east_y['NOTICE_DATE'].str[:10]
        east_y['UPDATE_DATE'] = east_y['UPDATE_DATE'].str[:10]
        east_y = east_y[['SECURITY_CODE', 'REPORT_DATE', 'NOTICE_DATE', 'UPDATE_DATE', 'PARENT_NETPROFIT']]
        if save == "y":  # net_profit
            with sqlite3.connect(gl_v.db_path) as conn:
                east_y.to_sql(code + 'east_y_fin', con=conn, if_exists='replace', index=False)

    @staticmethod  # east业绩预告
    def stock_yj_yg_em(save=''):
        import time
        """ http://data.eastmoney.com/bbsj/202003/yjyg.html"""
        li = ['20170331', '20170630', '20170930', '20171231', '20180331', '20180630', '20180930', '20181231',
              '20190331', '20190630', '20190930']
        # li = ['20200930','20201231','20210331','20210630','20210930','20211231','20220331','20220630','20220930']
        for i in li:
            east_y = ak.stock_yjyg_em(date=i)
            # east_y = east_y.dropna(axis=1, how='all')  # 去除空列
            east_y['公告日期'] = east_y['公告日期'].str[:10]  # 去除日期后面的0000
            if save == "y":
                with sqlite3.connect(gl_v.db_path) as conn:
                    east_y.to_sql('east_yj_yg', con=conn, if_exists='append', index=False)
            time.sleep(1.9)

    @staticmethod  # east机构调研-统计
    def stock_jg_dy_tj_em(save=''):
        # import time
        east_y = ak.stock_jgdy_tj_em(date="20170101")
        east_y = east_y.drop(labels=['最新价', '涨跌幅'], axis=1)  # 同时删除a,b列
        print(east_y)
        if save == "y":
            with sqlite3.connect(gl_v.db_path) as conn:
                east_y.to_sql('jg_dy_tj_em', con=conn, if_exists='append', index=False)
                # time.sleep(1.9)

    @staticmethod  # east机构调研详细
    def stock_jg_dy_detail_em(save=''):
        # import time
        east_y = ak.stock_jgdy_detail_em(date="20170101")
        east_y = east_y.drop(labels=['最新价', '涨跌幅'], axis=1)  # 同时删除a,b列
        # print(east_y)
        if save == "y":
            with sqlite3.connect(gl_v.db_path) as conn:
                east_y.to_sql('jg_dy_detail_em', con=conn, if_exists='append', index=False)
                # time.sleep(1.9)

    @staticmethod
    def stock_zh_a_gd_hs(save=''):
        """2022中报不齐全，http://data.eastmoney.com/gdhs/..东方财富网-数据中心-特色数据- 股东户数数据"""
        import time
        li = ['20130331', '20130630', '20130930', '20131231',
              '20140331', '20140630', '20140930', '20141231',
              '20150331', '20150630', '20150930', '20151231',
              '20160331', '20160630', '20160930', '20161231',
              '20170331', '20170630', '20170930', '20171231',
              '20180331', '20180630', '20180930', '20181231',
              '20190331', '20190630', '20190930', '20191231',
              '20200331', '20200630', '20200930', '20201231',
              '20210331', '20210630', '20210930', '20211231',
              '20220331', '20220630']
        for i in li:
            east_y = ak.stock_zh_a_gdhs(symbol=i)
            east_y = east_y.drop(labels=['最新价', '涨跌幅', '区间涨跌幅'], axis=1)  # 同时删除a,b列
            # print(east_y)
            if save == "y":
                with sqlite3.connect(gl_v.db_path) as conn:
                    east_y.to_sql('east_gd_hs', con=conn, if_exists='append', index=False)
                    time.sleep(1.5)


    @gl_v.time_show  # 循环银行码
    def for_code(self):
        import time
        li = """1601398
                0000001
                0002142
                1600000
                1600015
                1600016
                1600036
                1600919
                1600926
                1601009
                1601166
                1601169
                1601229
                1601288
                1601328
                1601658
                1601818
                1601838
                1601916
                1601939
                1601988
                1601998""".replace(' ', '').split('\n')
        for i in li:
            i = gl_v.add_sh(i, f='01')
            print(i)
            self.wy_stock_price(i, save='y')  # 网易获取未复权k线数据.流通市值、总市值
            self.east_finance(i, save='y')  # east年度利润表
            time.sleep(1)


stock_price = AkStockPrice()
# stock_price.for_code()   # 循环银行码
# stock_price.ak_stock_finance("601398", save='y')  # 新浪财务指标
# stock_price.df_stock_price_q_hfq("601398", save='y', adjust='hfq')  # 东风复权
# stock_price.stock_yj_yg_em(save='y')  # east业绩预告
# stock_price.stock_jg_dy_tj_em(save='y')  # east机构调研-统计
# stock_price.stock_jg_dy_detail_em(save='y')  # east机构调研详细
stock_price.stock_zh_a_gd_hs(save='y')  # 股东户数数据

"""2022中报不齐全，股东户数数据"""