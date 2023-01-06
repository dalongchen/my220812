import tools
import pandas as pd


class ToolsMain:  # 共用类

    def tool_li_day(self):  # 获取日期
        return [
            '2016-06-01',
            '2017-06-01',
            '2018-06-01',
            '2019-06-01',
            '2020-06-01',
            '2021-06-01',
            '2022-06-01',
        ]

    def tool_df(self):  # 获取ｃｓｖ数据
        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        return df

    def index_zh_a_hist_df(self):  # 获取全ａ历史数据
        import akshare as ak
        # print(ak.index_stock_cons_sina(symbol="399106"))
        path_ = tools.get_conn_cur(f='p')
        for ii in ['000001', '399106']:
            df = ak.index_zh_a_hist(
                symbol=ii, period="daily", start_date="19700101", end_date="22220101")
            print(df)
            df.to_csv(
                path_/'my_test_stock/jzcsyl/jzcsyl_csv/index{}.csv'.format(ii))

    @tools.time_show
    def stock_standard_k(self):  # 计算样本年涨幅
        conn, path_ = tools.get_conn_cur(f='conn_p')
        d_li = [
            '20150601_1_6',
            '20150601_7_11',
            '20150601_18',
        ]
        da = pd.DataFrame()
        for iii, dd in enumerate(d_li):
            # print(iii, dd)
            # return
            # sep分隔符，encoding编码header=None自动列名，names自定义列名，
            # index_col作为行索引的列（主键）,skiprows跳过行索引,na_values缺失值的替代字符串
            df = pd.read_csv(
                path_ /
                """my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl{}.csv""".format(dd),
                dtype={'code': object},
                sep=',',
                na_values=['NULL']
            )
            for i, t in df[['code', 'name']].iloc[0:].iterrows():
                # print(t)
                inp2 = tools.add_sh(t['code'], big="baostock")
                # # 查询单个票后复权数据并计算指定时间涨幅
                dat1 = tools.get_code_bfq(inp2, conn)
                dat1['jzcsyl'] = iii + 1
                dat1['jzc_type'] = dd
                # print(dat1)
                da1 = pd.concat([pd.DataFrame(t.values).T, dat1], axis=1)
                da = pd.concat([da, da1], axis=0, ignore_index=True)
        # print(da)
        # 结果集输出到文件
        # da.to_csv(path_/'my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv')
        conn.close()

    @tools.time_show
    def a_index_up(self):  # 计算指定时间全ａ指数年涨幅
        path_ = tools.get_conn_cur(f='p')
        lis = []
        for dd in ['000001', '399106']:
            df = pd.read_csv(
                path_/'my_test_stock/jzcsyl/jzcsyl_csv/index{}.csv'.format(dd),
                sep=',',
                na_values=['NULL'],
                index_col=0
            )
            close_front = df[df['日期'] == '2015-06-01']['收盘'].values[0]
            # print(close_front)
            for ddd in self.tool_li_day():
                close_h = df[df['日期'] >= ddd].iloc[0]['收盘']
                lis.append((close_h-close_front)/close_front)
        ii = int(len(lis)/2)
        dff = pd.DataFrame()
        dff['a'] = lis[0:ii]
        dff['b'] = lis[ii:]
        dff['y_up'] = (dff['a'] + dff['b'])/2
        # print(dff)
        return dff['y_up']

    @tools.time_show
    def box_plt():  # 净资产收益率和{}年后涨幅关系四分位图
        import matplotlib.pyplot as plt
        import seaborn as sns
        plt.rcParams['font.sans-serif'] = 'CESI_HT_GB13000'
        plt.rcParams['axes.unicode_minus'] = False
        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        ii = 0
        for gg in df.iloc[:, 3:-1]:
            ii += 1
            fig, ax = plt.subplots(1, 1, figsize=(8, 4))  # 1行2列的子图
            # 箱图显示均值，设置均值线属性
            ax1 = sns.boxplot(x='jzcsyl', y=gg, data=df, showmeans=True,
                              meanprops={'marker': 'D', 'markerfacecolor': 'red'})
            ax1.set_title('净资产收益率和{}年后涨幅关系四分位图'.format(ii), size=12)
            plt.savefig(
                path_/"my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601box{}.png".format(ii), dpi=100)
        # plt.show()

    @tools.time_show
    def describe_data():  # 净资产收益率{}年　数据描述
        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        for gg in df.iloc[:, 3:-1]:
            df1 = df[[gg, 'jzcsyl']]
            df_jzc = df1[df1['jzcsyl'] == 1]
            # print(df_jzc)
            # print(df_jzc[gg])
            print(df_jzc[gg].describe())  # 使用describe函数查看
            df_jzc = df1[df1['jzcsyl'] == 2]
            # print(df_jzc)
            print(df_jzc[gg].describe())
            df_jzc = df1[df1['jzcsyl'] == 3]
            # print(df_jzc)
            print(df_jzc[gg].describe())


# ToolsMain().index_zh_a_hist_df()
# ToolsMain().a_index_up()


class ParaTest:  # 参数检验,单＼双样本检验，ｆ检验－方差分析

    def zt_test(self):  # 正态检验, 正态\ｔ分布估计置信区间
        import scipy.stats as stats
        import statsmodels.api as sm
        # import numpy as np
        # # 创建一个随机数生成器（Random Number Generator）
        # rng = np.random.default_rng()
        # # rvs:随机变量（就是从这个分布中抽一些样本）生成n个服从正态分布的随机数,loc=期望, scale=标准差, size=生成随机数的个数
        # series_i = stats.norm.rvs(loc=5, scale=3, size=50, random_state=rng)
        # print(series_i)
        # """执行Shapiro-Wilk 正态性检验,原假设H0：样本数据服从正态分布"""
        # print(stats.shapiro(series_i))

        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        list_fac = df['jzcsyl'].unique()  # 分组标签取出
        for gg in df.iloc[:, 3:-2]:
            # print(stats.shapiro(df[gg]))  # 对每一年涨幅做正态性检验
            print(gg)
            for i in list_fac:
                series_i = df[df['jzcsyl'] == i][gg]
                """zconfint_mean方法可得到正态估计区间(下限[101.16400895]，上限[108.96099105])－＞９５％置信区间"""
                print(sm.stats.DescrStatsW(series_i).zconfint_mean(alpha=0.05))
                # t分布下的估计置信区间
                print(sm.stats.DescrStatsW(series_i).tconfint_mean(alpha=0.05))
                # t分布下的均值估计结果和sm.stats.DescrStatsW一致
                print(stats.bayes_mvs(series_i, alpha=0.95))
                # Shapiro-Wilk test, x 为待检测数据，返回统计量和P值,适合样本量小于50,
                # 当p值大于指定的显著性水平0.05，则接受原假设
                # print(list(series_i))
                # print(stats.shapiro(series_i))
                # Kolmogorov–Smirnov,K-S 检验，样本量适合50~300，x 待检测数据，cdf为待检验分布，
                # norm可检验正态，返回统计量和P值
                # stats.kstest (x, cdf, args = ( ), alternative ='two-sided', mode ='approx')
                # stats.anderson(x, dist ='norm' )  # x 为待检测数据，dist为待检测分布，可以正态、指数、二项等
                # stats.normaltest (a, axis=0) # 样本量大于300

    def z_test(self):  # 单,双样本总体z检验
        import statsmodels.stats.weightstats as ws
        import numpy as np
        # X = numpy.random.normal(-0.51, 0.05, 30).round(3)
        # 使用z检验
        # z, pval = ws.ztest(X, value=175)
        # print(ws.ztest(X, value=-0.5), X)
        """statsmodels.stats.weightstats.ztest(x1,x2,value=0,alternative="two-sided")
        可用于单样本和双样本均值的z检验，设置参数x2=none则为单样本的检验。"""
        tm = ToolsMain()
        a_up = tm.a_index_up()  # 计算指定时间全ａ指数年涨幅
        # print(a_up[1], a_up)
        df = tm.tool_df()  # 获取ｃｓｖ数据
        list_fac = df['jzcsyl'].unique()  # 分组标签取出
        ddd = 0
        for gg in df.iloc[:, 3:-2]:
            # print(a_up[ddd])
            print(gg)
            for i in list_fac:
                series_i = df[df['jzcsyl'] == i][gg]
                # smaller单样本ｚ检验 'larger' :   H1: difference in means larger than value
                print(np.round(ws.ztest(series_i, value=a_up[ddd], alternative='larger'), 3))
            ddd += 1

    def t_test(self):  # 单样本总体t检验
        # import numpy as np
        import statsmodels.api as sm

        # valueList = [0.169747, 0.165484, 0.142358, 0.143358, 0.141358, 0.0631967, 0.101527]
        # d = sm.stats.DescrStatsW(valueList)
        """alternative : str
            The alternative hypothesis, H1, has to be one of the following:
              - 'two-sided': H1: mean not equal to value (default)
              - 'larger' :   H1: mean larger than value
              - 'smaller' :  H1: mean smaller than value
        t检验只适合一个或两个样本的检验，而F检验适用于两个或多个样本的检验。
        """
        # print('t检验= %6.4f,p-value=%6.4f, df=%s' % d.ttest_mean(0.17))

        tm = ToolsMain()
        a_up = tm.a_index_up()  # 计算指定时间全ａ指数年涨幅
        # print(a_up[1], a_up)
        df = tm.tool_df()  # 获取ｃｓｖ数据
        list_fac = df['jzcsyl'].unique()  # 分组标签取出
        ddd = 0
        for gg in df.iloc[:, 3:-2]:
            # print(a_up[ddd])
            print(gg)
            for i in list_fac:
                series_i = df[df['jzcsyl'] == i][gg]
                d = sm.stats.DescrStatsW(series_i).ttest_mean(a_up[ddd], alternative='larger')
                print('t检验= %6.4f,p-value=%6.4f, df=%s' % d)
            ddd += 1

    def t_ttest_ind(self):  # 独立样本两总体参数t检验
        # import numpy as np
        import statsmodels.stats.weightstats as ws

        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        for gg in df.iloc[:, 3:-2]:
            # print(a_up[ddd])
            print(gg)
            se1 = df[df['jzcsyl'] == 1][gg]
            se2 = df[df['jzcsyl'] == 3][gg]
            """x1x2为两组样本数据，有相同的行列数shape；alternative为备择假设的形式，
            可选‘two-sided’双边检验, ‘larger’右尾检验, ‘smaller’左尾检验；
            usevar是否要求方差齐性，pooled要求， unequal 不要求；value指定原假设取等号时的检验值"""
            d = ws.ttest_ind(se2, se1, alternative='larger', usevar='unequal', value=0)
            print(d)

    @tools.time_show
    def fc_levene():  # 用Levene方法分别对各因素进行方差齐性检验并解释结果
        import numpy as np
        from scipy import stats   # 里面有方差齐性检验方差

        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        df['2018-06-01'] = 1/df['2018-06-01']
        df['2019-06-01'] = 1/df['2019-06-01']
        # df['2020-06-01'] = df['2020-06-01']
        df['2021-06-01'] = 1/df['2021-06-01']
        # print(df['2020-06-01'])
        # 用Levene方法分别对各因素进行方差齐性检验并解释结果
        # for gg in df.iloc[:, 3:-2]:
        for gg in df.iloc[:, 3:-2]:
            df_c = df[[gg, 'jzcsyl']]
            d1 = df_c[df_c['jzcsyl'] == 1][gg].values
            d2 = df_c[df_c['jzcsyl'] == 2][gg].values
            d3 = df_c[df_c['jzcsyl'] == 3][gg].values
            # 样本数据，可能具有不同的长度。只接受一维样本
            year_jzc = stats.levene(*[d1, d2, d3])
            print(gg, np.round(year_jzc, 2))
        # P=0.0 净资产收益率的三组数据之间有显著差异。2016-06-01 [0.23  0.795]

    def f_oneway(self):  # 单因素方差分析
        from scipy.stats import f_oneway
        """scipy.stats的f模块可调用各种显著性水平下，对应的两个自由度的F临界值。
        scipy.stats.f.ppf(1-显著性水平，自由度1，自由度2)"""

        # data1 = [29.6, 24.3, 28.5, 32.0]
        # data2 = [27.3, 32.6, 30.8, 34.8]
        # data3 = [5.8, 6.2, 11.0, 8.3]
        # data4 = [21.6, 17.4, 18.3, 19.0]
        # data5 = [29.2, 32.8, 25.0, 24.2]
        # f, p = f_oneway(data1, data2, data3, data4, data5)
        # print("F检验值={},p值={}".format(f, p))

        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        for gg in df.iloc[:, 3:-2]:
            # print(a_up[ddd])
            print(gg)
            ser1 = df[df['jzcsyl'] == 1][gg]
            ser2 = df[df['jzcsyl'] == 2][gg]
            ser3 = df[df['jzcsyl'] == 3][gg]
            f, p = f_oneway(ser1, ser2, ser3)
            print("F检验值={},p值={}".format(f, p))

    @tools.time_show
    def anova_ols(self):  # 最小二乘法拟合 方差分析
        import pandas as pd
        from statsmodels.stats.anova import anova_lm
        from statsmodels.formula.api import ols

        data1 = [29.6, 24.3, 28.5, 32.0]
        data2 = [27.3, 32.6, 30.8, 34.8]
        data3 = [5.8, 6.2, 11.0, 8.3]
        data4 = [21.6, 17.4, 18.3, 19.0]
        data5 = [29.2, 32.8, 25.0, 24.2]
        # 由于5组数据都是一个变量的不同水平，需要合并成一列数据，方便算法计算
        num = sorted(["d1", "d2", "d3", "d4", "d5"]*4)  # 扩展4行名称并排序
        data = data1+data2+data3+data4+data5  # 将5组数据合并成一个list
        df = pd.DataFrame({"num": num, "data": data})
        print(df)
        mod = ols("data~num", data=df).fit()
        res = anova_lm(mod, typ=2)  # 2表示dataframe，有1,2,3这三个字，2代表dataframe格式
        print(res)
        # df表示自由度，sum_sq表示离差平方和，F表示F检验值，PR表示p值，residul表示残差
        return
        from statsmodels.formula.api import ols  # 最小二乘法拟合
        from statsmodels.stats.anova import anova_lm  # 方差分析
        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        df['2018-06-01'] = 1/df['2018-06-01']
        df['2019-06-01'] = 1/df['2019-06-01']
        # df['2020-06-01'] = df['2020-06-01']
        df['2021-06-01'] = 1/df['2021-06-01']
        df.rename(
            columns={
                "2016-06-01": "a2016",
                "2017-06-01": "a2017",
                "2018-06-01": "a2018",
                "2019-06-01": "a2019",
                "2020-06-01": "a2020",
                "2021-06-01": "a2021",
                "2022-06-01": "a2022",
            },
            inplace=True
        )
        # print(df)
        # for gg in df.iloc[:, 3:-8]:
        # for gg in df.iloc[:, -3:-2]:
        for gg in df.iloc[:, 3:-2]:
            print(gg)
            # C指的是Categorical variables　截距项intercept
            model = ols('{} ~C(jzcsyl)'.format(gg), data=df).fit()
            # model = ols('{} ~jzc_type'.format(gg), data=df[[gg, 'jzc_type']]).fit()
            print(model.summary())
            # anova_table = anova_lm(model, type=1)
            # print(pd.DataFrame(anova_table))
        # print(df)
        """
        -formula是回归的公式 y~x左边为因变量，右边为自变量
        -data为使用的数据，必须是pandas.DataFrame格式
        p值是概率，表示某一事件发生的可能性大小。如果P值很小，说明原假设情况的发生的概率很小，
        而如果出现了，根据小概率原理，我们就有理由拒绝原假设，P值越小，我们拒绝原假设的理由越充分。
        总之，P值越小，表明结果越显著。
        """


# ParaTest().zt_test()　　# 正态检验, 正态\ｔ分布估计置信区间
# ParaTest().z_test()  # z检验
# ParaTest().t_test()  # t检验
# ParaTest().t_ttest_ind()  # 两总体参数t检验
# ParaTest().f_oneway()  # 单因素方差分析
ParaTest().anova_ols()  # 单因素方差分析ols最小二乘法拟合 


class Test:  # 非参数检验
    def ppr(self):
        print(self)
        print(self.__class__)

# t = Test()
# t.ppr()
