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
                stock_df.to_sql(code+adjust, con=conn, if_exists='replace', index=False)

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
        east_y = east_y[['SECURITY_CODE','REPORT_DATE','NOTICE_DATE','UPDATE_DATE','PARENT_NETPROFIT']]
        # print(east_y)
        # print(east_y.dtypes)
        if save == "y":  # net_profit
            with sqlite3.connect(gl_v.db_path) as conn:
                east_y.to_sql(code + 'east_y_fin', con=conn, if_exists='replace', index=False)

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

# stock_price = AkStockPrice()
# stock_price.for_code()


# stock_price.ak_stock_finance("601398", save='y')  # 新浪财务指标


