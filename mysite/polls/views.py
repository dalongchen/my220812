# from dis import code_info
from django.http import JsonResponse
from .mytool import tool_db, tools, tool_east
import pandas as pd
import time


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
    inp2 = tools.add_sh(inp, big='east.')
    print(question_id, inp2)
    conn, cur = tool_db.get_conn_cur()
    """'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude',
     'up_change', 'num_change', 'turnover'"""
    sql = r"""select * from '{}' where volume != 0""".format('east'+inp2+'_2')
    try:
        dat2 = pd.read_sql(sql, conn)
    except Exception as e:
        # print(e)
        if 'no such table' in str(e):
            print(inp2, '没有后复权表数据,正下载east')
            tool_east.east_history_k_data(inp2, 2, save='y')  # fq=1前，=2后复权
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
                'east'+inp2+'_2', data), conn)
            if not dat_high.empty:
                return dat_high.values[0]
            else:
                sql_high2 = r"""select date,high from '{}' where date>'{}'"""
                dat_high = pd.read_sql(sql_high2.format(
                    'east'+inp2+'_2', data), conn)
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
def vote(request, question_id):
    import json
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
    #     return JsonResponse({
    #     'col': col,
    #     'da':df_no_total.values.tolist(),
    #     'code2':df_no_total['股票代码'].values.tolist(),
    #     'name2':df_no_total['股票简称'].values.tolist()
    # })
    # return HttpResponse("You're voting on question %s." % question_id)


@tools.time_show
def vote1(request, question_id):
    import json
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
    sql_high_china = r"""select 日期,开盘,收盘,最高,最低,涨跌幅 from '{}' where 日期>='{}'
    LIMIT {}"""
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
            notice_day = yj_change[:4]+'-04-10'
            print(yj_change[:4])
        elif '1-6' in yj_change:
            notice_day = yj_change[:4]+'-07-10'
            print(notice_day)
        elif '1-9' in yj_change:
            notice_day = yj_change[:4]+'-10-10'
            print(notice_day)
        elif '1-12' in yj_change:
            notice_day = yj_change[:4]+'-01-10'
            print(notice_day)
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
                if dat20_up_change > 5:
                    # nnn+=1
                    # print(nnn,t['股票代码'])
                    df_up1[i] = t
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
    print('涨幅大于某设定值得数据:', df_up1)
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
    #     return JsonResponse({
    #     'col': col,
    #     'da':df_no_total.values.tolist(),
    #     'code2':df_no_total['股票代码'].values.tolist(),
    #     'name2':df_no_total['股票简称'].values.tolist()
    # })
    # return HttpResponse("You're voting on question %s." % question_id)
