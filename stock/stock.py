import numpy as np
import pandas as pd
import sqlite3
import global_variable as gl_v


# 获取可转债正股股价
class Stock(object):
    def __init__(self):
        pass

    @gl_v.time_show  # '股本',总市值收益率
    def calculate_equity(self, code, save=''):
        with sqlite3.connect(gl_v.db_path) as conn:
            # data_s = pd.read_sql(r"select 日期,收盘价,前收盘,总市值 from '{}'".format(code), conn)
            # data_s = self.calculate_equity_add(data_s)  # '股本',
            # 总市值
            data_s = pd.read_sql(r"select 日期,总市值 from '{}'".format(code), conn)
            # 年度净利润 PARENT_NETPROFIT
            net_y = pd.read_sql("select REPORT_DATE,PARENT_NETPROFIT from '{}' ".format(code+'east_y_fin'), conn)
            df_re = self.total_value_rate(data_s, net_y)  # 总市值收益率
            if save == 'y':
                df_re.to_sql(code + 'my', con=conn, if_exists='replace', index=False)
                # self.calculate_save(code, conn, data_s)

    @gl_v.time_show  # '股本',  calculate_equity1 :  0.020001 sec,, .values= 0.007second
    def calculate_equity_add(self, data_s):
        data_s.loc[data_s.收盘价 == 0, '收盘价'] = data_s.loc[data_s.收盘价 == 0, '前收盘']
        data_s['股本'] = data_s['总市值'].values / data_s['收盘价'].values
        # data_s['股本增比'] = 1
        # data_s.loc[1:, '股本增比'] = data_s['股本'][:-1].values/data_s['股本'][1:].values
        # print(data_s.loc[0, '日期'])
        start_d_e = data_s.loc[0, ['日期', '股本']].values
        return data_s

    @gl_v.time_show  # 总市值收益率
    def total_value_rate(self, data_s, net_y):
        net_y = net_y.loc[net_y.REPORT_DATE > str(int(data_s.loc[0].values[0][:4])-1)].values[::-1]
        np_ar = []
        # print(np_ar)
        for i in net_y:
            # print(i, int(i[0][:4])+1, int(i[0][:4])+2)
            # hh = data_s.loc[(data_s.日期 > str(int(i[0][:4])+1)) & (data_s.日期 < str(int(i[0][:4])+2)), ['日期','总市值']]
            # hh = data_s.loc[(data_s.日期 > str(int(i[0][:4])+1)) & (data_s.日期 < str(int(i[0][:4])+2)), '总市值']
            hh = i[1]/data_s.loc[(data_s.日期 > str(int(i[0][:4])+1)) & (data_s.日期 < str(int(i[0][:4])+2)), '总市值'].values
            np_ar += list(hh)
        data_s['total_v_rate'] = np_ar
        data_s['total_v_rate'] = data_s['total_v_rate'].round(3)
        return data_s[['日期', 'total_v_rate']]

    def calculate_save(self, code, conn, data_s):  # 保存
        cur = conn.cursor()
        """加列 REAL"""
        # sql_col = r"""alter table '{}' add 股本增比 REAL""".format(code)
        # sql_col = r"""alter table '{}' add 股本 number(16)""".format(code)
        # sql_col = r"""ALTER TABLE '{}' DROP COLUMN 股本""".format(code)
        # cur.execute(sql_s)
        """save"""
        sql_s = r"UPDATE '{}' SET 股本=(?),股本增比=(?) WHERE 日期=(?)".format(code)
        # print(sql_s)
        data_s = data_s[['股本', '股本增比', '日期']]
        cur.executemany(sql_s, data_s.values)

    @gl_v.time_show  # 循环银行码
    def for_code(self):
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
        1601998""".replace(' ', '').split('\n')[2:]
        for i in li:
            # i = gl_v.add_sh(i, f='01')
            print(i)
            self.calculate_equity(gl_v.add_sh(i, f='01'), save='y')  #

# stock = Stock()
# stock.for_code()



