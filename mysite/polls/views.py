from django.http import JsonResponse
from .mytool import tool_db, tools, tool_east, tool_akshare, tool_baostock
import pandas as pd
import time
import json
# from . import views_son
import datetime


def index(request):
    sql = """SELECT 股票代码,股票简称,预测指标,业绩变动,预测数值,业绩变动幅度 as 幅度,
    业绩变动原因,预告类型 as 类型,上年同期值,公告日期 FROM polls_east_yj_yg as p WHERE
    p."预告类型" = '预增' AND
    p."业绩变动" LIKE '%2020年1-6%' AND
    p."股票代码" NOT LIKE '688%' AND
    p."股票代码" NOT LIKE '900%' AND
    p."股票代码" NOT LIKE '83%' AND
    p."股票代码" NOT LIKE '200%' AND
    p."业绩变动原因" IS NOT NULL AND
    p."预测指标" NOT LIKE '%营业收入%' AND
    p."上年同期值" IS NOT NULL
    GROUP BY
    p."公告日期",
    p."股票代码"
    """
    conn, cur = tool_db.get_conn_cur()
    df = pd.read_sql(sql, conn)
    col = []
    for i, t in enumerate(df.columns):
        col.append({
            'name': i,
            'align': 'left',
            'label': t,
            'field': i,
            'sortable': True,
            'style': 'padding: 0px 0px',
            'headerStyle': 'padding: 0px 0px'})
    conn.close()
    # print(df['股票代码'].values.tolist())
    return JsonResponse({'col': col, 'da': df.values.tolist(),
                         'code2': df['股票代码'].values.tolist()})


def detail(request, question_id):
    if request.method == 'GET':
        tota = request.GET.get('tota', default='110')
        print(tota)
        conn, cur = tool_db.get_conn_cur()
        sql = r"""select * from polls_east_yj_yg where {}""".format(tota)
        dat = cur.execute(sql)
        conn.close()
    return JsonResponse({'dat': dat})


@tools.time_show
def results(request, question_id):  # k线图和预增提示
    inp = request.GET.get('inp', default='11')
    # inp2 = tools.add_sh(inp, big='east.')
    print(question_id, inp)
    conn, cur = tool_db.get_conn_cur()
    sql = r"""select * from '{}'""".format('east'+inp+'_2')
    try:
        dat2 = pd.read_sql(sql, conn)
    except Exception as e:
        # print(e)
        if 'no such table' in str(e):
            print(inp, '没有后复权表数据,正下载east')
            tool_east.east_history_k_data(inp, 2, save='y')  # fq=1前，=2后复权
            dat2 = pd.read_sql(sql, conn)
            time.sleep(1.3)
        else:
            raise e
    dat2.insert(4, 'i', dat2.index.tolist())
    dat2['max'] = dat2.apply(lambda x: 1 if x['close']
                             > x['open'] else -1, axis=1)

    sql_yj_yg = r"""select * from polls_east_yj_yg where 预告类型='预增' and 股票代码 =
    '{}' GROUP BY 公告日期""".format(inp)
    dat_yj_yg = pd.read_sql(sql_yj_yg, conn)
    if dat_yj_yg['最高价'].isnull().sum() > 0:
        print('业绩预告无数据')
        sql_high = r"""select date,high from '{}' where date='{}'"""

        def datacheck(data):
            dat_high = pd.read_sql(sql_high.format(
                'east'+inp+'_2', data), conn)
            if not dat_high.empty:
                return dat_high.values[0]
            else:
                sql_high2 = r"""select date,high from '{}' where date>'{}'"""
                dat_high = pd.read_sql(sql_high2.format(
                    'east'+inp+'_2', data), conn)
                if not dat_high.empty:
                    return dat_high.head(1).values[0]
                else:
                    raise
                    return ['', '']
        dd = dat_yj_yg['公告日期'].apply(datacheck)
        dat_yj_yg[['显示日期', '最高价']] = list(dd)
        dat_yj_yg[['code']] = inp
        sql_s = r"""UPDATE polls_east_yj_yg SET 显示日期 = (?), 最高价 = (?) where
        公告日期 = (?) and 股票代码 = (?)"""
        data_s = dat_yj_yg[['显示日期', '最高价', '公告日期', 'code']]
        cur.executemany(sql_s, data_s.values)
        conn.commit()
        dat_yj_yg = pd.read_sql(sql_yj_yg, conn)

    conn.close()
    dat_yj_yg = dat_yj_yg[['显示日期', '最高价', '公告日期',
                           '预告类型', '预测数值', '业绩变动', '业绩变动原因']]

    return JsonResponse({
        'categoryData': dat2.date.values.tolist(),
        'values': dat2[[
            'close',
            'open',
            'high',
            'low',
            'volume',
            'amount',
            'amplitude',
            'up_change',
            'num_change',
            'turnover']].values.tolist(),
        'volumes': dat2[['i', 'volume', 'max']].values.tolist(),
        'dat_yj_yg': dat_yj_yg.values.tolist()
    })


@tools.time_show
def stock_standard_k(request, question_id):  # 标准k线图和?提示
    inp = request.GET.get('inp', default='11')
    conn = tool_db.get_conn_cur()
    inp2 = tools.add_sh(inp, big="baostock")
    # 查询单个票后复权数据
    dat2 = tool_akshare.get_code_bfq(inp2, conn, fq='hfq')
    # 查询有没有这个表
    # dat = pd.read_sql(
    # """select name from sqlite_master where type='table' and
    # name like '%{}'""".format(inp + 'hfq'), conn)
    # print(inp, dat)
    # if dat.shape[0] == 1:
    #     dat2 = pd.read_sql(
    #         r"""select * from '{}'""".format(dat['name'].values[0]), conn)
    # else:
    #     print('没有这个表:', inp)
    #     inp2 = tools.add_sh(inp, big="baostock")
    #     # 查询单个票后复权数据
    #     dat2 = tool_akshare.get_code_bfq(inp2, conn, fq='hfq')
    conn.close()
    # 控制成交金额颜色转换和k线同步
    dat2.insert(0, 'i', dat2.index.tolist())
    dat2.insert(
        0,
        'max',
        dat2.apply(lambda x: 1 if x['close'] > x['open'] else -1, axis=1)
    )
    return JsonResponse({
        'categoryData': dat2.date.values.tolist(),
        'values': dat2.iloc[:, 3:].round(2).values.tolist(),
        'volumes': dat2[['i', 'amount', 'max']].values.tolist(),
        # 'dat_yj_yg': dat_yj_yg.values.tolist()
    })


@tools.time_show
def vote(request, question_id):  # 业绩预告
    quarter = request.GET.get('quarter', default='11')
    quarter = json.loads(quarter).get('_value')
    print(quarter)
    # breakpoint()
    # quarter=request.GET.get('quarter',default='11')rr
    len = int(request.GET.get('day_num', default='11'))
    up_num = int(request.GET.get('up_num', default='11'))
    down_num = int(request.GET.get('down_num', default='11'))
    print(question_id, quarter, len, up_num, down_num)
    # num_total = 0  # 大于0.15的数量
    # num2_total = 0  # 小于-0.3的数量
    df_no_total = pd.DataFrame()
    df_have_total = pd.DataFrame()
    df_up1 = pd.DataFrame()  # 上涨预设幅度的股票

    sql = """SELECT 股票代码,股票简称,预测指标,业绩变动,预测数值,业绩变动幅度 as 幅度,
        业绩变动原因,预告类型 as 类型,上年同期值,公告日期  FROM polls_east_yj_yg as p WHERE
        p."预告类型" = '预增' AND
        p."业绩变动" LIKE '%{}%' AND
        p."公告日期" <= '{}' AND
        p."股票代码" NOT LIKE '688%' AND
        p."股票代码" NOT LIKE '900%' AND
        p."股票代码" NOT LIKE '8%' AND
        p."股票代码" NOT LIKE '200%' AND
        p."股票代码" NOT LIKE '4%' AND
        p."股票代码" NOT LIKE '001235' AND
        p."业绩变动原因" IS NOT NULL AND
        p."预测指标" NOT LIKE '%营业收入%' AND
        p."上年同期值" IS NOT NULL
        GROUP BY
        p."公告日期",
        p."股票代码"
        """
    conn, cur = tool_db.get_conn_cur()
    up1 = up_num/100  # 幅度5/100=0.05
    up2 = down_num/100  # 幅度
    num = 0  # 大于0.15的数量
    num2 = 0  # 小于-0.15的数量
    # 获取股价sql
    sql_high = r"""select date,open,close,high,low,up_change from '{}' where
     date>='{}' LIMIT {}"""
    sql_high_china = r"""select 日期,开盘,收盘,最高,最低,涨跌幅 from '{}' where
     日期>='{}' LIMIT {}"""
    # 获取前一季度也预告的股票sql
    sql_continuity = """SELECT * FROM polls_east_yj_yg as p WHERE
    p."股票代码" = '{}' AND
    p."业绩变动" LIKE '%{}%' and (
    p."预告类型" LIKE '预增' or
    p."预告类型" LIKE '略增' or
    p."预告类型" LIKE '扭亏' )
    """

    # nnn = 0
    for iii in quarter:
        print(iii.split(','), '=======================')
        iii = iii.split(',')
        yj_change = iii[1]
        yj_change2 = iii[0]
        if '1-3' in yj_change:
            notice_day = yj_change[:4]+'-04-01'
            # print(yj_change[:4])
        elif '1-6' in yj_change:
            notice_day = yj_change[:4]+'-07-01'
            # print(notice_day)
        elif '1-9' in yj_change:
            notice_day = yj_change[:4]+'-10-01'
            # print(notice_day)
        elif '1-12' in yj_change:
            notice_day = yj_change[:4]+'-01-01'
            # print(notice_day)
        else:
            print('error')
            raise

        df = pd.read_sql(sql.format(yj_change, notice_day), conn)
        df_no = df.copy(deep=True)  # 前一季度没有
        df_have = df.copy(deep=True)  # 前一季度有

        for i, t in df.iterrows():
            dat1 = pd.read_sql(sql_continuity.format(
                t['股票代码'], yj_change2), conn)
            if dat1.shape[0] != 0:  # 不等于0,说明前面季度有预增
                df_no.drop([i], inplace=True)  # 删除有预增的,剩没有预增的
            else:   # 等于0,说明前面季度没有预增
                df_have.drop([i], inplace=True)  # 删除没有预增的,剩有预增的

                # 获取没有预增的的code,计算股价胜率
                inp2 = tools.add_sh(t['股票代码'], big='east.')
                try:
                    dat2 = pd.read_sql(sql_high.format(
                        'east'+inp2+'_2', t['公告日期'], len), conn)
                    open2 = 'open'
                    close2 = 'close'
                    up_change = 'up_change'
                    if dat2.shape[0] < len:
                        print(inp2, '表数据不足{}'.format(len))

                except Exception as e:
                    if 'no such table' in str(e):
                        try:
                            dat2 = pd.read_sql(sql_high_china.format(
                                t['股票简称']+t['股票代码']+'hfq', t['公告日期'], len),
                                conn)
                        except Exception as e2:
                            if 'no such table' in str(e2):
                                print(t['股票简称'], t['股票代码'], '没有后复权表数据,正下载east')
                                tool_east.history_k_single(
                                    t['股票简称'], t['股票代码'], conn, save='y',
                                    end_date='20221109')  # fq=1前，=2后复权
                                time.sleep(1.35)
                                dat2 = pd.read_sql(sql_high_china.format(
                                    t['股票简称']+t['股票代码']+'hfq', t['公告日期'], len),
                                    conn)
                            else:
                                raise e2
                        open2 = '开盘'
                        close2 = '收盘'
                        up_change = '涨跌幅'
                    else:
                        raise e
                # print(dat2.loc[0])
                dat20_open = dat2.loc[0][open2]
                dat20_up_change = dat2.loc[0][up_change]
                if dat20_up_change < 5 and dat20_up_change > 0:
                    # nnn += 1
                    df_up1 = pd.concat([df_up1, t], axis=1)
                    for ind, row in dat2.iterrows():
                        up_ = (row[close2]-dat20_open)/dat20_open
                        if up_ > up1:
                            num += 1
                            break
                        if up2 != 0.01:  # 负跌幅为1时
                            if up_ < up2:
                                num2 += 1
                                break

        df_no_total = pd.concat([df_no_total, df_no], axis=0)
        df_have_total = pd.concat([df_have_total, df_have], axis=0)
        # print('没连续:',df_no.shape,'有连续:',df_have.shape)

    df_up1 = pd.DataFrame(
        df_up1.values.T, index=df_up1.columns, columns=df_up1.index)
    print('涨幅大于某设定值得数据:', df_up1.shape)
    # print(num,'{}内天至少有一天大于{}的概率:'.format(len,up1),num/df_no.shape[0],'----------------------')
    print(num, num/df_up1.shape[0], '{}内天至少有一天大于{}的概率:'.format(len,
          up1), num/df_no_total.shape[0], '----------------------')
    if up2 != 0.01:  # 负跌幅为1时
        print(num2, '{}内天小于{}的概率:'.format(len, up2), num2/df_no.shape[0])
        print('money:', num*up1+num2*up2, (num*up1+num2*up2)/df_no.shape[0])

    print('没连续total:', df_no_total.shape, '有连续total:', df_have_total.shape)

    conn.close()
    col = []
    for i, t in enumerate(df_no.columns):
        col.append({'name': i, 'align': 'left', 'label': t, 'field': i,
                    'sortable': True, 'style': 'padding: 0px 0px',
                    'headerStyle': 'padding: 0px 0px'})
    return JsonResponse({
        'col': col,
        'da': df_up1.values.tolist(),
        'code2': df_up1['股票代码'].values.tolist(),
        'name2': df_up1['股票简称'].values.tolist()
    })


@tools.time_show
def stock_yjbb_em(request):  # 基本面 净资产收益率,总资产收益率,分季度
    quarter = request.GET.get('quarter', default='11')
    quarter = json.loads(quarter).get('_value')
    # print(quarter)
    # breakpoint()
    len = int(request.GET.get('day_num', default='11'))
    up_num = int(request.GET.get('up_num', default='11'))
    down_num = int(request.GET.get('down_num', default='11'))
    print(quarter, len, up_num, down_num)
    new_concat = tool_akshare.stock_yjbb_em_20(quarter, len)
    new_concat['营业收入'] = (new_concat['营业收入']/100000000).round(2)
    new_concat['营收同比'] = (new_concat['营收同比']).round(2)
    new_concat['营收季度环比'] = (new_concat['营收季度环比']).round(2)
    new_concat['净利润'] = (new_concat['净利润']/100000000).round(2)
    new_concat['净利同比'] = (new_concat['净利同比']).round(2)
    new_concat['净利季度环比'] = (new_concat['净利季度环比']).round(2)
    new_concat['毛利率'] = (new_concat['毛利率']).round(2)
    # print((new_concat['净利润']/100000000).round(2))
    col = []
    for i, t in enumerate(new_concat.columns):
        col.append({'name': i, 'align': 'left', 'label': t, 'field': i,
                    'sortable': True, 'style': 'padding: 0px 0px',
                    'headerStyle': 'padding: 0px 0px'})
    return JsonResponse({
        'col': col,
        'da': new_concat.values.tolist(),
        'code2': new_concat['股票代码'].values.tolist(),
        'name2': new_concat['股票简称'].values.tolist()
    })


@tools.time_show  # 获取某天价值股,净资产收益率，总资产收益率，ｐｅ＜２１
def jia_zhi(request):
    quarter = request.GET.get('quarter', default='11')
    # print(strftime1, strftime2, "日期", strftime1 <= strftime2)
    print(quarter)
    # 4444 查询低ｐｅ＜41股票中前一年去年ｐｅ＜4１的股票,净资产收益率大于0.1
    sql_pe1 = """select code,close,peTTM from '{}' where code in {}
        and date>'{}' and date<'{}'
        and (cast(pbMRQ as decimal(10,2))/cast(peTTM as decimal(10,2)))>0.01
        and (cast(pbMRQ as decimal(10,2))/cast(peTTM as decimal(10,2)))>0.06
        """
    day2 = quarter.replace('/', '-')
    yy_ = int(day2[:4]) - 1
    year_front = []
    for g in range(4):  # 获取４年日期
        yy_g = str(yy_-g)
        year_front.append((yy_g + '-04-01', yy_g + '-04-10', g*10 + 91))
    # print(year_front)
    strftime1 = datetime.datetime.strptime(day2, "%Y-%m-%d")
    strftime2 = datetime.datetime.strptime("2022-12-10", "%Y-%m-%d")
    conn = tool_db.get_conn_cur()
    if strftime1 <= strftime2:
        print(day2)
        # print(sql_pe.format(t['name'], day2, 21, 0))
        # 查询低ｐｅ＜２１股票 333333333
        # 净资产收益率cast(pbMRQ as decimal(10,2))/cast(peTTM as decimal(10,2))>0.2
        dat_pe = pd.read_sql(
            """select code,name,open,close,high,low,volume,amount,turn,
            pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM from '{}' where date='{}' and
            cast(peTTM as decimal(10,2))<{} and cast(peTTM as decimal(10,2))>{}
            and (cast(pbMRQ as decimal(10,2))/cast(peTTM as decimal(10,2)))>{}
            and (cast(pbMRQ as decimal(10,2))/cast(peTTM as decimal(10,2)))<{}
            """.format(
                'baostock_day_k2022-12-10', day2, 91, 0, 0.01, 0.06), conn
            )
        dat_pe = dat_pe.replace('', 0)
        dat_pe.iloc[:, 2:] = dat_pe.iloc[:, 2:].astype(float)
        # print(dat_pe)
        for x in year_front:
            # print(x)
            dat_pe_code = tuple(dat_pe['code'])
            # 查询符合ｄａｔ＿ｐｅ<21的前一年数据
            dat_pe1 = pd.read_sql(
                sql_pe1.format(
                    'baostock_day_k2022-12-10', dat_pe_code, x[0], x[1]),
                conn
            )
            dat_pe1[["close", "peTTM"]] = dat_pe1[[
                "close", "peTTM"]].astype(float)
            # print(dat_pe1.dtypes)
            # 计算下年每股收益
            dat_pe1['shou_yi1'] = dat_pe1['close']/dat_pe1['peTTM']
            # 去重
            dat_pe1 = dat_pe1.drop_duplicates(subset='code')
            # print('qu_cong', dat_pe1)
            # 默认内链接，取交集
            dat_pe1 = pd.merge(
                dat_pe,
                dat_pe1[['code', 'shou_yi1']],
                on=['code']
            )
            # 选出市盈率符合数组里面设定要求的市盈率
            dat_pe = dat_pe1[dat_pe1['close']/dat_pe1['shou_yi1'] < x[2]]
            del dat_pe['shou_yi1']
        # print(dat_pe)
        # 计算符合年总资产收益率的股票 22222222222
        # dat_pe = views_son.cal_zzcsyl(quarter, conn, dat_pe)
        dat_pe['code'] = dat_pe['code'].str[3:]
        # dat_pe = dat_pe.copy()
        dat_pe.iloc[:, 2:] = dat_pe.iloc[:, 2:].round(2)
        # print(dat_pe)
        # 结果集输出到文件 11111111
        # dat_pe.to_csv('jzcsyl20150601_1_6.csv')
        conn.close()
        return tools.view_return_response(dat_pe, JsonResponse)
    else:
        raise


@tools.time_show  # 获取某天涨停股,技术股
def zhang_ting(request):
    quarter = request.GET.get('quarter', default='11')
    print(quarter)
    # quarter = json.loads(quarter).get('_value')
    new_concat = (tool_akshare.ak_zhang_ting(quarter)).iloc[:, 1:]
    if new_concat.shape[0] > 0:
        new_concat.iloc[:, 2:8] = (new_concat.iloc[:, 2:8]).round(2)
        # new_concat['涨跌幅'] = (new_concat['涨跌幅']).round(2)
        col = []
        for i, t in enumerate(new_concat.columns):
            col.append({'name': i, 'align': 'left', 'label': t, 'field': i,
                        'sortable': True, 'style': 'padding: 0px 0px',
                        'headerStyle': 'padding: 0px 0px'})
        return JsonResponse({
            'col': col,
            'da': new_concat.values.tolist(),
            'code2': new_concat['代码'].values.tolist(),
            'name2': new_concat['名称'].values.tolist()
        })
    else:
        print('非交易日?没有数据')


@tools.time_show  # 更新history day k线数据和在交易股票表
def update_day_k(request):
    quarter = request.GET.get('quarter', default='11')
    # quarter = json.loads(quarter).get('_value')
    print(quarter.replace('/', ''))
    fq = request.GET.get('fq', default='11')
    # fq = json.loads(fq).get('_value')
    # print(fq)
    conn = tool_db.get_conn_cur()
    if fq == '更新交易股票':
        tool_akshare.stock_info_a_code_name_df(conn)  # 更新交易股票数据
    elif fq == 'baostock历史k':
        print(fq)
        # 查询股票中文名""
        dat = pd.read_sql(
            """select * from stock_info_a_code_name where code like '00%'
                or code like '30%' or code like '60%'""",
            conn
        )
        tool_baostock.baostock_history_k(dat, quarter, conn)
    elif fq == 'east历史k':  # 前台页面已经作废的方法
        return
        print(fq)
        # 查询股票中文名
        sql_china_name = """select * from stock_info_a_code_name
        where code like '00%' or code like '30%' or code like '60%'"""
        dat = pd.read_sql(sql_china_name, conn)
        # print(dat)
        for i, t in dat.iloc[0:].iterrows():
            print(i, t['name'].replace(' ', '').replace('*', ''), t['code'])
            tool_akshare.history_k_single(
                t['name'].replace(' ', '').replace('*', ''),
                t['code'],
                conn=conn,
                save='y',
                end_date=quarter,
                fq=''
            )
            time.sleep(0.5)
    elif fq == '不复权':
        return
        print(fq)
        # baostock不复权日ｋ
        tool_baostock.baostock_bfq_k(quarter, conn)
    elif fq == '后复权':
        return
        print(fq)
    elif fq == '不/后复':
        return
        print('oop')
        # 实时行情转入不复权数据表
        tool_akshare.stock_zh_a_spot_em_to_bfq(save='y', day=quarter)
        tool_akshare.hfq_calu_total(fq2='hfq', flat='pp')  # 计算后复权数据表
    else:
        print('error')
    conn.close()
    return JsonResponse({
            'col': [],
            'da': [],
            'code2': [],
            'name2': []
        })
