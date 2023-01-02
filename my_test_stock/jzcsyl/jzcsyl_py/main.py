import tools
import pandas as pd


@tools.time_show
def stock_standard_k():  # 计算年涨幅
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
            path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl{}.csv""".format(dd),
            dtype={'code': object},
            sep=',',
            na_values=['NULL']
        )
        for i, t in df[['code', 'name']].iloc[0:].iterrows():
            # print(t)
            inp2 = tools.add_sh(t['code'], big="baostock")
            # 查询单个票后复权数据
            dat1 = tools.get_code_bfq(inp2, conn, d='2016-06-01', fq='hfq')
            dat1['jzcsyl'] = iii + 1
            # print(dat1)
            da1 = pd.concat([pd.DataFrame(t.values).T, dat1], axis=1)
            da = pd.concat([da, da1], axis=0, ignore_index=True)
    # print(da)
    # 结果集输出到文件
    # da.to_csv(path_/'my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv')
    conn.close()


# stock_standard_k()


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
        plt.savefig(path_/"my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601box{}.png".format(ii), dpi=100)
    # plt.show()


# box_plt()


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


# describe_data()


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
    # 用Levene方法分别对各因素进行方差齐性检验并解释结果
    for gg in df.iloc[:, 3:-1]:
        # print(df['jzcsyl'], df[gg])
        year_jzc = stats.levene(df['jzcsyl'], df[gg])
        print(gg, np.round(year_jzc, 3))
    # P=0.0 净资产收益率的三组数据之间有显著差异。


# fc_levene()


@tools.time_show
def anova_ols():  # 最小二乘法拟合 方差分析
    from statsmodels.formula.api import ols  # 最小二乘法拟合
    from statsmodels.stats.anova import anova_lm  # 方差分析
    path_ = tools.get_conn_cur(f='p')
    df = pd.read_csv(
        path_/"""my_test_stock/jzcsyl/jzcsyl_csv/jzcsyl20150601_up.csv""",
        dtype={'code': object},
        sep=',',
        na_values=['NULL']
    )
    for gg in df.iloc[:, 3:-1]:
        print(df['jzcsyl'], df[gg])
    # print(df)
    # 对教育程度和职业进行方差分析，对结果进行解释，分析这两个因素对对数收入是否有显著影响以及有怎样的影响。
    # 需要考虑因素的交互作用。
    return
    model = ols('ln_income ~career + education + career:education', data=df).fit()
    # 或者 model = ols('ln_income ~career*education', data = df).fit()
    anova_table = anova_lm(model, type=2)
    pd.DataFrame(anova_table)


# anova_ols()
