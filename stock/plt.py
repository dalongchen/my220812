import pandas as pd
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
import global_variable as gl_v

# matplotlib不支持显示中文的 显示中文需要一行代码设置字体
mpl.rcParams['font.family'] = 'SimHei'
plt.rcParams['axes.unicode_minus'] = False   # 步骤二（解决坐标轴负数的负号显示问题）


class StockPlt(object):

    @staticmethod  # 银行投资收益率比
    def total_value_rate(tab_s):
        with sqlite3.connect(gl_v.db_path) as conn:
            df_s = pd.read_sql(r"select 日期,total_v_rate from '{}' ".format(tab_s), conn, parse_dates=["日期"])
            # df_s = pd.read_sql(r"select 日期,total_v_rate from '{}' where 日期>='2006-10-27'".format(tab_s), conn)
            df_s = df_s.set_index('日期')
            plt.figure(figsize=(15, 8))
            sns.lineplot(x="日期", y="total_v_rate", data=df_s)
            plt.title('{}历史投资收益率比'.format(tab_s[:-2]))
            # plt.show()
            # plt.savefig(r'D:\myzq\axzq\T0002\stock_load\my_stock\bank_picture\{}.png'.format(tab_s[:-2]), dpi=600, format='png')

    # @gl_v.time_show  # 银行柱状图
    def sea_bar(self):
        import numpy as np
        # 生成测试数据
        x = np.linspace(0, 10, 2)
        y = 11 - x
        # 绘制柱状图
        plt.bar(x, y)
        # 循环，为每个柱形添加文本标注
        # 居中对齐
        zz = ['假设','工商']
        for xx, yy, zzz in zip(x, y, zz):
            plt.text(xx, yy + 0, str(zzz), ha='center')

        # 显示图形
        plt.show()

        # bank_l = gl_v.bank_li.split(',')
        # # print(bank_l)
        # with sqlite3.connect(gl_v.db_path) as conn:
        #     bank_l2 = []
        #     for i in bank_l:
        #         df_b = pd.read_sql("""select total_v_rate from '{}' order by "日期" desc limit 0,1""".format(i + 'my'),
        #                            conn)
        #         bank_l2.append(df_b.values[0, 0])
        # # print(bank_l2)
        # plt.figure(figsize=(18, 7))
        # plt.bar(bank_l, bank_l2, color='b')
        # plt.title('银行收益率对比图 ')
        # # plt.show()
        # plt.savefig(r'D:\myzq\axzq\T0002\stock_load\my_stock\bank_picture\{}.png'.format('500亿银行'), dpi=600, format='png')

    @staticmethod  # 历史波动率和价格
    def hist_v_close(tab_s):
        with sqlite3.connect(gl_v.db_path) as conn:
            df_s = pd.read_sql(r"select 日期,y_v_change from '{}' ".format(tab_s), conn, parse_dates=["日期"]).dropna()
            # print(df_s[pd.isna(df_s['y_v_change']) == True])
            # print(df_s[pd.isnull(df_s['y_v_change']) == True])
            df_s = df_s.set_index('日期')
            plt.figure(figsize=(15, 8))
            sns.lineplot(data=df_s['y_v_change'])
            plt.title('历史年波动率 ')
            # sns.lineplot(data=df_s[['y_v_change', 'close']])
            # plt.title('历史波动率和收盘价对比图')
            plt.show()

    @staticmethod  # 蒙特卡洛下隐含波动率,历史波动率、bs隐含波动率和价格
    def mc_implication_v_log_close(df_b, name):
        if name == '大参':
            # sns.lineplot(data=df_b[['optimal_mc_vol', 'y_v_log']])
            # plt.title('{}蒙特卡洛隐含波动率、历史波动率对比图'.format(name))
            # sns.lineplot(data=df_b[['optimal_mc_vol', 'implication_volatility', 'y_v_log']])
            # plt.title('{}蒙特卡洛隐含波动率、bs隐含波动率,历史波动率'.format(name))
            sns.lineplot(data=df_b[['optimal_mc_vol', 'implication_volatility', 'y_v_log', 'close']])
            plt.title('{}隐含波动率、bs隐含波动率、历史波动率和转债收盘价'.format(name))
        if name == '柳药':
            # sns.lineplot(data=df_b[['optimal_mc_vol', 'y_v_log']])
            # plt.title('{}蒙特卡洛隐含波动率、历史波动率对比图'.format(name))
            # sns.lineplot(data=df_b[['optimal_mc_vol', 'implication_volatility', 'y_v_log']])
            # plt.title('{}蒙特卡洛隐含波动率、bs隐含波动率,历史波动率'.format(name))
            sns.lineplot(data=df_b[['optimal_mc_vol', 'implication_volatility', 'y_v_log', 'close']])
            plt.title('{}隐含波动率、bs隐含波动率、历史波动率和转债收盘价'.format(name))
        plt.show()

    @staticmethod  # 债券价格和bs计算的债券价值
    def bound_close_bs_value(df_b):
        sns.lineplot(data=df_b[['bs_option_kzz', 'close']])
        # plt.title('大参实际债券价格和bs算法价值')
        plt.title('柳药实际债券价格和bs算法价值')
        plt.show()

    @staticmethod  # 普通蒙特卡洛价值
    def bound_close_mc_value(df_b):
        sns.lineplot(data=df_b[['general_mc', 'bs_option_kzz', 'close']])
        # plt.title('大参蒙特卡洛价值和bs算法价值')
        plt.title('柳药实际债券价格和bs算法价值')
        plt.show()

    @staticmethod  # 最优停时蒙特卡洛价值
    def optimal_close_mc_value(df_b):
        sns.lineplot(data=df_b[['optimal_mc', 'general_mc', 'bs_option_kzz', 'close']])
        # plt.title('大参最优停时蒙特卡洛价值')
        plt.title('柳药最优停时蒙特卡洛价值')
        plt.show()

    @staticmethod  # 是否归一化
    def sea_history_implication_volatility(tab_b, tab_s, f_normal):
        with sqlite3.connect(gl_v.db_path) as conn:
            sql_b = """select date,close,bs_option_kzz,general_mc,optimal_mc,implication_volatility from '{}'""".format(
                tab_b)
            df_b = pd.read_sql(sql_b, conn, parse_dates=["date"])
            date_1 = df_b['date'].head(1).apply(lambda x: x.strftime('%Y-%m-%d'))[0]
            print(date_1)
            sql_s = """select close, y_v_log from '{}' where date>='{}'""".format(tab_s, date_1)
            df_s = pd.read_sql(sql_s, conn)
            df_b['s_close'] = df_s['close']
            df_b['y_v_log'] = df_s['y_v_log']
            df_b = df_b.set_index('date')
            df_b = df_b.astype('float')
            if f_normal == 'y':   # 是否归一化
                df_b = (df_b - df_b.min()) / (df_b.max() - df_b.min())  # 归一化
            plt.figure(figsize=(15, 8))

            dd = "implication_v_log_close"
            if dd == "implication_v_log_close":  # 隐含波动率,历史波动率和债券价格
                KzzSeaBorn.implication_v_log_close(df_b)
            if dd == "bound_close_bs_value":  # 债券价格和bs计算的债券价值
                KzzSeaBorn.bound_close_bs_value(df_b)
            if dd == "bound_close_mc_value":  # 普通蒙特卡洛价值
                KzzSeaBorn.bound_close_mc_value(df_b)
            if dd == "optimal_close_mc_value":  # 普通蒙特卡洛价值
                KzzSeaBorn.optimal_close_mc_value(df_b)

    @staticmethod  # 蒙特卡洛下隐含波动率 f_normal：是否归一化
    def mc_history_implication_volatility(tab_b, tab_s, f_normal, name):
        with sqlite3.connect(gl_v.db_path) as conn:
            sql_b = """select date,close,implication_volatility,optimal_mc_vol from '{}'""".format(
                tab_b)
            df_b = pd.read_sql(sql_b, conn, parse_dates=["date"])
            date_1 = df_b['date'].head(1).apply(lambda x: x.strftime('%Y-%m-%d'))[0]
            # print(date_1)
            sql_s = """select close, y_v_log from '{}' where date>='{}'""".format(tab_s, date_1)
            df_s = pd.read_sql(sql_s, conn)
            df_b['s_close'] = df_s['close']
            df_b['y_v_log'] = df_s['y_v_log']
            df_b = df_b.set_index('date')
            df_b = df_b.astype('float')
            if f_normal == 'y':  # 是否归一化
                df_b = (df_b - df_b.min()) / (df_b.max() - df_b.min())  # 归一化
            plt.figure(figsize=(15, 8))

            dd = "mc_implication_v_log_close"
            if dd == "mc_implication_v_log_close":  # 隐含波动率,历史波动率和债券价格
                KzzSeaBorn.mc_implication_v_log_close(df_b, name)

    @staticmethod  # 画收益率直方图，检验是否正态分布。
    def histogram_or_is_normal_dis(tab_b, tab_s):
        import scipy.stats as scs
        with sqlite3.connect(gl_v.db_path) as conn:
            se_sql = r"select date,log_ from '{}'".format(tab_s)
            df_b = pd.read_sql(se_sql, conn, parse_dates=["date"])
            # df_b = df_b.set_index('date')
            df_b = df_b['log_'][-500:] / 100
            print('Norm test p-value %14.3f' % scs.normaltest(df_b)[1])
            # v = np.random.normal(size=300)
            # print(scs.normaltest(v))
            norm = scs.norm.rvs(loc=0, scale=1, size=1000)  # rvs表示生成指定分布的分布函数
            print(scs.normaltest(norm))
            plt.figure(figsize=(15, 8))
            plt.hist(df_b, bins=40, edgecolor='k')
            plt.title('Title using Matplotlib ')
            plt.show()

    # 循环银行码
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
        1601998""".replace(' ', '').split('\n')
        for i in li:
            # i = gl_v.add_sh(i, f='01')
            print(i)
            self.total_value_rate(gl_v.add_sh(i, f='01')+'my')  #

if __name__ == '__main__':
    ksb = StockPlt()
    # ksb.for_code()  # 银行投资收益率比
    # ksb.sea_bar()  # 银行柱状图
    ksb.hist_v_close(tab_s='sh601398my')

    # ksb.sea_history_implication_volatility(tab_b=gl_v.da_sen_b, tab_s=gl_v.da_sen_s, f_normal='y')  # 大参
    # ksb.sea_history_implication_volatility(tab_b=gl_v.liu_yao_b, tab_s=gl_v.liu_yao_s, f_normal='')  # 柳药

    # 蒙特卡洛下隐含波动率
    # ksb.mc_history_implication_volatility(tab_b=gl_v.da_sen_b, tab_s=gl_v.da_sen_s, f_normal='y', name='大参')  # 大参
    # ksb.mc_history_implication_volatility(tab_b=gl_v.liu_yao_b, tab_s=gl_v.liu_yao_s, f_normal='y', name='柳药')

    # ksb.histogram_or_is_normal_dis(tab_b=gl_v.liu_yao_b, tab_s=gl_v.liu_yao_s)  # 柳药