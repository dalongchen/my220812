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

    def tool_df(self, f=''):  # 获取ｃｓｖ数据
        path_ = tools.get_conn_cur(f='p')
        df = pd.read_csv(
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        if f == '':
            return df
        if f == 2:
            return df, path_

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
    def box_plt(self, f=''):  # 净资产收益率和{}年后涨幅关系四分位图，ｆ＝２画直方图
        import matplotlib.pyplot as plt
        import seaborn as sns
        plt.rcParams['font.sans-serif'] = 'CESI_HT_GB13000'
        plt.rcParams['axes.unicode_minus'] = False
        df, path_ = self.tool_df(f=2)
        ii = 0
        # for gg in df.iloc[:, 3:-2]:
        for gg in df.iloc[:, -3:-2]:
            if f == '':  # 画箱线图
                ii += 1
                fig, ax = plt.subplots(1, 1, figsize=(8, 4))  # 1行2列的子图
                # 箱图显示均值，设置均值线属性
                ax1 = sns.boxplot(x='jzcsyl', y=gg, data=df, showmeans=True,
                                  meanprops={'marker': 'D', 'markerfacecolor': 'red'})
                ax1.set_title('净资产收益率和{}年后涨幅关系四分位图'.format(ii), size=12)
                # plt.savefig(
                #     path_/"my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601box{}.png".format(ii), dpi=100)
            if f == 2:  # 画直方图
                d1 = df[df['jzcsyl'] == 1][gg].values
                d2 = df[df['jzcsyl'] == 2][gg].values
                d3 = df[df['jzcsyl'] == 3][gg].values
                sns.displot(d1, kde=True)
                plt.title(gg + '数据集1')
                sns.displot(d2, kde=True)
                plt.title(gg + '数据集2')
                sns.displot(d3, kde=True)
                plt.title(gg + '数据集3')
                plt.show()

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


# ToolsMain().index_zh_a_hist_df()  # 获取全ａ历史数据
# ToolsMain().a_index_up()  # 计算指定时间全ａ指数年涨幅
# ToolsMain().box_plt(f=2)


class ParaTest:  # 参数检验,单＼双样本检验，ｆ检验－方差分析

    def zt_test(self, f):  # 正态检验, 正态\ｔ分布估计置信区间
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

        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        list_fac = df['jzcsyl'].unique()  # 分组标签取出
        for gg in df.iloc[:, 3:-2]:
            # print(stats.shapiro(df[gg]))  # 对每一年涨幅做正态性检验
            print(gg)
            for i in list_fac:
                series_i = df[df['jzcsyl'] == i][gg]
                if f == 'mean':  # 估计置信区间
                    """zconfint_mean方法可得到正态估计区间(下限[101.16400895]，上限[108.96099105])－＞９５％置信区间"""
                    # print(sm.stats.DescrStatsW(series_i).zconfint_mean(alpha=0.05))
                    # # t分布下的估计置信区间
                    print(sm.stats.DescrStatsW(series_i).tconfint_mean(alpha=0.05))
                    # t分布下的均值估计结果和sm.stats.DescrStatsW一致
                    print(stats.bayes_mvs(series_i, alpha=0.95))
                if f == 'shapiro':  # 正态性检验
                    # Shapiro-Wilk test, x 为待检测数据，返回统计量和P值,适合样本量小于50,
                    # 当p值大于指定的显著性水平0.05，则接受原假设
                    # data = [87, 77, 92, 68, 80, 78, 84, 77, 81, 80, 80, 77, 92, 86,
                    #         76, 80, 81, 75, 77, 72, 81, 72, 84, 86, 80, 68, 77, 87,
                    #         76, 77, 78, 92, 75, 80, 78]
                    print(stats.shapiro(series_i))
                    # return

    @tools.time_show
    def fc_levene(self, f):  # 用Levene方法分别对各因素进行方差齐性检验
        import numpy as np
        from scipy import stats   # 里面有方差齐性检验方差
        # 这里是随机生成的数据，相同的条件，有一定的概率出现齐和不齐的矛盾
        # A = stats.norm.rvs(loc=1, scale=1.1, size=(35)).round(4)  # 生成A公司的销售额
        # B = stats.norm.rvs(loc=1.2, scale=1, size=(30)).round(4)  # 生成B公司的销售额
        # print(list(A), list(B))  # 进行levene检验
        # A = [1.0942, 1.1501, 3.5823, 0.9121, 1.3506, 1.1824, 1.3453, 2.1039, 0.3804, 0.5935, 0.1457,
        # 0.8254, 1.8466, 2.0999, 1.4345, 1.5848, 1.0598, 1.6722, 0.9101, 0.5517, 1.1808, 1.8105,
        # 1.0371, 1.1012, 0.6243, 1.1696, 2.0166, 0.1301, 1.747, -0.6323, -0.3689, 1.551, 1.4555,
        # 1.5964, 2.756]
        # B = [-1.1653, -0.8151, 0.9861, 0.7797, 0.9308, 3.4523, 2.2472, -1.4611, -0.1066, -0.0009,
        # 1.0455, 3.0819, 0.8077, 2.6612, 0.8867, 2.6037, 0.4398, -0.9041, -1.4055, 0.4753, 3.0384,
        # 1.8281, 1.2435, 1.9684, 2.6899, 0.8439, 3.1615, -0.925, 0.126, 2.0406]
        # 方差有差异LeveneResult(statistic=11.37349093148534, pvalue=0.0012778283554916124)
        # A = [0.3886, 1.1425, 1.0865, -0.4129, -0.0549, 0.0986, 1.4797, 0.5632, 1.368, 1.4655,
        # 0.4392, 2.3143, 0.6491, 1.0754, 1.2782, 2.596, 1.0671, -0.7807, 0.9889, 1.4671, 0.998,
        # 0.8704, 1.4502, 2.7007, 0.014, 1.9202, 0.8085, 1.6934, 0.9389, 3.3939, -0.339, 0.5745,
        # -0.0397, 2.4227, 1.0813]
        # B = [1.1416, 3.1655, 1.4725, 1.1772, 1.1752, 1.7364, 1.418, 2.2786, 0.9255, 1.9141,
        # 0.5345, 1.25, 1.5529, 1.1268, 3.0391, 0.8253, 1.9717, 0.8799, 1.9872, 1.6706, 2.9861,
        # 1.9165, 0.6177, 0.2849, 3.5311, 1.658, 2.0414, -0.9646, -0.5122, 0.7878]
        # 方差没有差异LeveneResult(statistic=0.09986197295986858, pvalue=0.7530399226692273)
        # print(stats.levene(A, B))  # 进行levene检验
        # 方差不同，设为equal_var=False
        # print(stats.ttest_ind(A, B, equal_var=False))  # 进行独立样本t检验
        # return
        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        # df['2018-06-01'] = 1/df['2018-06-01']
        # df['2019-06-01'] = 1/df['2019-06-01']
        # # df['2020-06-01'] = df['2020-06-01']
        # df['2021-06-01'] = 1/df['2021-06-01']
        # print(df['2020-06-01'])
        # 用Levene方法分别对各因素进行方差齐性检验并解释结果
        if f == 2:
            aa = {}
            # for gg in df.iloc[:, 3:-2]:
            for gg in df.iloc[:, 3:-2]:
                df_c = df[[gg, 'jzcsyl']]
                d1 = df_c[df_c['jzcsyl'] == 1][gg].values
                d3 = df_c[df_c['jzcsyl'] == 3][gg].values
                # 样本数据，可能具有不同的长度。只接受一维样本
                year_jzc = stats.levene(*[d1, d3])
                # print(gg, np.round(year_jzc, 2))
                aa[gg] = year_jzc[1]
            # print(aa)
            return aa
        if f == 3:
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

    def z_test(self):  # 单,双样本z检验
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

    def t_test(self):  # 单样本总体t检验-非参数检验
        import statsmodels.api as sm

        # from scipy import stats
        # data = pd.Series([15.6, 16.2, 22.5, 20.5, 16.4,
        #                  19.4, 16.6, 17.9, 12.7, 13.9])
        # data_mean = data.mean()
        # data_std = data.std()
        # pop_mean = 20
        # t, p_towTail = stats.ttest_1samp(data, pop_mean)
        # print(data_mean, data_std, t, p_towTail)

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

    def t_ttest_ind(self):  # 独立样本两总体参数t检验-非参数检验
        # import numpy as np
        import statsmodels.stats.weightstats as ws
        bb = self.fc_levene(f=2)  # f=2比较２个
        # print(bb)

        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        for gg in df.iloc[:, 3:-2]:
            if bb[gg] > 0.05:  # 方差齐
                us = 'pooled'
            else:
                us = 'unequal'
            print(gg)
            se1 = df[df['jzcsyl'] == 1][gg]
            se2 = df[df['jzcsyl'] == 3][gg]
            """x1x2为两组样本数据，有相同的行列数shape?alternative为备择假设的形式，
            可选‘two-sided’双边检验, ‘larger’右尾检验, ‘smaller’左尾检验；
            usevar是否要求方差齐性， pooled 要求， unequal 不要求；value指定原假设取等号时的检验值"""
            # d = ws.ttest_ind(se2, se1, alternative='larger', usevar='unequal', value=0)
            d = ws.ttest_ind(se2, se1, alternative='larger', usevar=us, value=0)
            print(d)

    def f_oneway(self):  # 单因素方差分析
        from scipy.stats import f_oneway
        """scipy.stats的f模块可调用各种显著性水平下，对应的两个自由度的F临界值。
        scipy.stats.f.ppf(1-显著性水平，自由度1，自由度2)
        方差分析ANOVA-F检验
        需要数据满足以下两个基本前提：
        数据独立
        各观测变量总体要服从正态分布
        各观测变量的总体满足方差齐
        """

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
        # import pandas as pd
        from statsmodels.stats.anova import anova_lm  # 方差分析
        from statsmodels.formula.api import ols  # 最小二乘法拟合

        # data1 = [29.6, 24.3, 28.5, 32.0]
        # data2 = [27.3, 32.6, 30.8, 34.8]
        # data3 = [5.8, 6.2, 11.0, 8.3]
        # data4 = [21.6, 17.4, 18.3, 19.0]
        # data5 = [29.2, 32.8, 25.0, 24.2]
        # # 由于5组数据都是一个变量的不同水平，需要合并成一列数据，方便算法计算
        # num = sorted(["d1", "d2", "d3", "d4", "d5"]*4)  # 扩展4行名称并排序
        # data = data1+data2+data3+data4+data5  # 将5组数据合并成一个list
        # df = pd.DataFrame({"num": num, "data": data})
        # # print(df)
        # mod = ols("data~num", data=df).fit()
        # res = anova_lm(mod, typ=2)  # 2表示dataframe，有1,2,3这三个字，2代表dataframe格式
        # print(res)
        # df表示自由度，sum_sq表示离差平方和，F表示F检验值，PR表示p值，residul表示残差
        # return
        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据

        # df['2018-06-01'] = 1/df['2018-06-01']
        # df['2019-06-01'] = 1/df['2019-06-01']
        # # df['2020-06-01'] = df['2020-06-01']
        # df['2021-06-01'] = 1/df['2021-06-01']
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
            # print(pd.DataFrame(anova_lm(model, type=2)))
        # print(df)
        """
        -formula是回归的公式 y~x左边为因变量，右边为自变量
        -data为使用的数据，必须是pandas.DataFrame格式
        sum_sq表示离差平方和
        p值是概率，表示某一事件发生的可能性大小。如果P值很小，说明原假设情况的发生的概率很小，
        而如果出现了，根据小概率原理，我们就有理由拒绝原假设，P值越小，我们拒绝原假设的理由越充分。
        总之，P值越小，表明结果越显著。
        """


# ParaTest().fc_levene(f=2)  # f=2比较２个
# ParaTest().zt_test(f='mean')  # 正态检验, 正态\ｔ分布估计置信区间
# ParaTest().zt_test(f='shapiro')  # 正态检验, 正态\ｔ分布估计置信区间
# ParaTest().z_test()  # z检验
# ParaTest().t_test()  # 单样本t检验
# ParaTest().t_ttest_ind()  # 两总体参数t检验
# ParaTest().f_oneway()  # 单因素方差分析
# ParaTest().anova_ols()  # 单因素方差分析ols最小二乘法拟合


class NotParaTest:  # 非参数检验

    def zt_kstest(self):  # kstest正态检验,是否同分布
        from scipy import stats
        """各阶段两组数据的累计概率分布差值的最大值.Kolmogorov–Smirnov,K-S 检验，样本量适合50~300，x 待检测数据，
        cdf为待检验分布，返回统计量和P值,结果返回两个值statistic → D值，pvalue → p值大于0.05，为正态分布"""

        # import numpy as np
        # # 创建随机生成器
        # rng = np.random.default_rng()
        """stats.norm.cdf(α,均值,方差)：累积概率密度函数-cumulative distribution function
        uniform.rvs-均匀分布"""
        # x = stats.uniform.rvs(size=100, random_state=rng)
        # x = stats.norm.rvs(size=100, loc=0, random_state=rng)
        # x = stats.norm.rvs(size=100, random_state=rng)
        # stats.norm.cdf默认为标准正态分布
        # print(stats.kstest(x, stats.norm.cdf))
        # return
        # 拉普拉斯分布
        # sample1 = stats.laplace.rvs(size=105, random_state=rng)
        # sample2 = stats.laplace.rvs(size=95, random_state=rng)
        # print(stats.kstest(sample1, sample2))
        # return

        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        list_fac = df['jzcsyl'].unique()  # 分组标签取出
        for gg in df.iloc[:, 3:-2]:
            # print(stats.shapiro(df[gg]))  # 对每一年涨幅做正态性检验
            print(gg)
            for i in list_fac:
                ser = df[df['jzcsyl'] == i][gg]
                u = ser.mean()  # 计算均值
                std = ser.std()  # 计算标准差
                # print("样本均值为：%.2f，样本标准差为：%.2f" % (u,std))
                print(stats.kstest(ser, 'norm', (u, std)))
                # print(stats.kstest(ser, stats.norm.cdf))
                # return

    def kruskal_test(self):  # Kruskal-Wallis 检验
        from scipy import stats
        A = [1, 3, 6, 9, 0]
        B = [3, 5, 1, 4, 11, 34]
        C = [1, 9, 5, 3, 0, 2, 4, 5, 7, 12]
        kw = stats.kruskal(A, B, C)
        print(kw)

    def chisquare_test(self):  # 卡方检验
        from scipy import stats

        A = [1, 3, 6, 9, 0]
        B = [3, 5, 12, 54, 11]
        chi = stats.chisquare(A, B)
        print(chi)


# NotParaTest().zt_kstest()  # kstest正态检验
# NotParaTest().kruskal_test()  # Kruskal-Wallis 检验
NotParaTest().chisquare_test()  # 卡方检验


class MultiComp:  # 多重比较
    """由于是双样本假设检验，有时候刚刚接触该方法的读者容易直接将两个总体当做样本来进行检验，那得到的结论是错误的，
    如：两天就生产了个10个产品，直接将这10个产品的某参数进行了双样本的假设检验。其实如果总体数比较少的时候，
    无需统计估计，可以直接比较均值?"""

    def multi_comp(self):  #
        from statsmodels.stats.multicomp import MultiComparison
        tm = ToolsMain()
        df = tm.tool_df()  # 获取ｃｓｖ数据
        """true有差异。false没有差异"""
        for gg in df.iloc[:, 3:-2]:
            print(gg)
            mc = MultiComparison(df[gg], groups=df['jzcsyl'])
            print(mc.tukeyhsd(alpha=0.05))


# MultiComp().multi_comp()  #
