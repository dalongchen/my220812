from dis import code_info
from django.http import JsonResponse
from .mytool import tool_db,tools,tool_east
import pandas as pd
import time


def index(request):
    sql ="""SELECT 股票代码,股票简称,预测指标,业绩变动,预测数值,业绩变动幅度 as 幅度,
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
    conn,cur=tool_db.get_conn_cur()
    df = pd.read_sql(sql, conn )
    col = []
    for i,t in enumerate(df.columns):
        col.append({ 'name': i, 'align': 'left', 'label': t, 'field': i, 'sortable': True, 
        'style': 'padding: 0px 0px', 'headerStyle':'padding: 0px 0px' })
    conn.close()
    # print(df['股票代码'].values.tolist())
    return JsonResponse({'col': col, 'da':df.values.tolist(),'code2':df['股票代码'].values.tolist()})

def detail(request, question_id):
    if request.method=='GET':
        tota=request.GET.get('tota',default='110')
        print(tota)
        conn,cur=tool_db.get_conn_cur()
        sql = r"""select * from polls_east_yj_yg where {}""".format(tota)
        dat = cur.execute(sql)
        conn.close()
    return JsonResponse({'dat':dat})

@tools.time_show
def results(request, question_id):  # k线图和预增提示
    inp=request.GET.get('inp',default='11')
    inp2 = tools.add_sh(inp, big='east.')
    print(question_id, inp2)  
    conn,cur=tool_db.get_conn_cur()
    """'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'up_change', 
    'num_change', 'turnover'"""
    sql = r"""select * from '{}' where volume != 0""".format('east'+inp2+'_2')
    try:
        dat2 = pd.read_sql(sql, conn )
    except Exception as e:
        # print(e)
        if 'no such table' in str(e):
            print(inp2,'没有后复权表数据,正下载east')
            tool_east.east_history_k_data(inp2,2,save='y')  # fq=1前，=2后复权
            dat2 = pd.read_sql(sql, conn )
            time.sleep(1.3)
        else:
            raise e
    dat2.insert(4,'i',dat2.index.tolist()) 
    dat2['max'] = dat2.apply(lambda x: 1 if x['close'] > x['open'] else -1, axis=1)
    
    """CREATE TABLE "polls_east_yj_yg" (
	"股票代码"	,
	"股票简称"	,
	"预测指标"	,
	"业绩变动"	,
	"预测数值"	,
	"业绩变动幅度"	,
	"业绩变动原因"	,
	"预告类型"	,
	"上年同期值"	,
	"公告日期"	
    )"""
    
    sql_yj_yg = r"""select * from polls_east_yj_yg where 预告类型='预增' and 股票代码 = '{}' GROUP BY 公告日期""".format(inp)
    dat_yj_yg = pd.read_sql(sql_yj_yg,conn)
    if dat_yj_yg['最高价'].isnull().sum()>0:
        print('业绩预告无数据')
        sql_high = r"""select date,high from '{}' where date='{}'"""
        def datacheck(data):
            dat_high = pd.read_sql(sql_high.format('east'+inp2+'_2',data), conn )
            if not dat_high.empty:
                return dat_high.values[0]
            else:
                sql_high2 = r"""select date,high from '{}' where date>'{}'"""
                dat_high = pd.read_sql(sql_high2.format('east'+inp2+'_2',data), conn )
                if not dat_high.empty:
                    return dat_high.head(1).values[0]
                else:
                    raise
                    return  ['','']
        dd=dat_yj_yg['公告日期'].apply(datacheck)
        dat_yj_yg[['显示日期','最高价']]=list(dd)
        dat_yj_yg[['code']]=inp
        sql_s = r"UPDATE polls_east_yj_yg SET 显示日期=(?),最高价=(?) where  公告日期=(?) and 股票代码=(?)"
        data_s = dat_yj_yg[['显示日期', '最高价', '公告日期','code']]
        cur.executemany(sql_s, data_s.values)
        conn.commit()
        dat_yj_yg = pd.read_sql(sql_yj_yg,conn)

    conn.close()
    dat_yj_yg = dat_yj_yg[['显示日期', '最高价', '公告日期','预告类型','预测数值','业绩变动','业绩变动原因']]
    
    return JsonResponse({
        'categoryData': dat2.date.values.tolist(),
        'values':dat2[['close', 'open',  'high', 'low', 'volume','amount', 'amplitude', 'up_change', 'num_change', 'turnover']].values.tolist(),
        'volumes':dat2[['i','volume','max']].values.tolist(),
        'dat_yj_yg':dat_yj_yg.values.tolist()
    })

@tools.time_show
def vote(request, question_id):
    quarter=request.GET.get('quarter',default='11').split(',')
    day_num=int(request.GET.get('day_num',default='11'))
    up_num=int(request.GET.get('up_num',default='11'))
    down_num=int(request.GET.get('down_num',default='11'))
    print(question_id, quarter, day_num,up_num, down_num) 
    yj_change=quarter[1]
    yj_change2=quarter[0]
    sql ="""SELECT 股票代码,股票简称,预测指标,业绩变动,预测数值,业绩变动幅度 as 幅度,
    业绩变动原因,预告类型 as 类型,上年同期值,公告日期  FROM polls_east_yj_yg as p WHERE
    p."预告类型" = '预增' AND
    p."业绩变动" LIKE '%{}%' AND
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
    """.format(yj_change)
    conn,cur=tool_db.get_conn_cur()
    df = pd.read_sql(sql, conn )
    df_no = df.copy(deep=True)  # 前一季度没有
    df_have = df.copy(deep=True)  # 前一季度有

    len=day_num
    up1=up_num/100 # 幅度
    up2=down_num/100 # 幅度
    num = 0  # 大于0.01的数量
    num2 = 0  # 小于-0.3的数量
    result = [0 for i in range(0,len)]  # 定义一个初值为0的数组
    # print(result)
    # 获取股价sql
    sql_high = r"""select date,open,close,high,low from '{}' where date>='{}' LIMIT {}"""
    # 获取前一季度也预告的股票sql
    sql_continuity ="""SELECT * FROM polls_east_yj_yg as p WHERE
    p."股票代码" = '{}' AND
    p."业绩变动" LIKE '%{}%' and (
    p."预告类型" LIKE '预增' or
    p."预告类型" LIKE '略增' or
    p."预告类型" LIKE '扭亏' )
    """
    for i,t in df.iterrows():
        dat2 = pd.read_sql(sql_continuity.format(t['股票代码'],yj_change2), conn )
        if dat2.shape[0]!=0:  # 不等于0,说明前面季度有预增
            df_no.drop([i],inplace=True)  # 删除有预增的,剩没有预增的
        else:   # 等于0,说明前面季度没有预增
            df_have.drop([i],inplace=True)  # 删除没有预增的,剩有预增的

            # 获取没有预增的的code,计算股价胜率
            inp2 = tools.add_sh(t['股票代码'], big='east.')
            try:
                dat2 = pd.read_sql(sql_high.format('east'+inp2+'_2',t['公告日期'], len), conn )
                # print(dat2.shape,dat2.shape[0]<len)
                if dat2.shape[0]<len:
                    print(inp2,'表数据不足{},正下载east'.format(len))
                    tool_east.east_history_k_data(inp2,2,save='y')  # fq=1前，=2后复权
                    time.sleep(1.35)
                    dat2 = pd.read_sql(sql_high.format('east'+inp2+'_2',t['公告日期'],len), conn )
                    if dat2.shape[0]<len:
                        print(inp2,'表数据不足{},新股?'.format(len))

            except Exception as e:
                if 'no such table' in str(e):
                    print(inp2,'没有后复权表数据,正下载east')
                    tool_east.east_history_k_data(inp2,2,save='y')  # fq=1前，=2后复权
                    time.sleep(1.35)
                    dat2 = pd.read_sql(sql_high.format('east'+inp2+'_2',t['公告日期'],len), conn )
                else:
                    raise e
            
            dat20_open=dat2.loc[0]['open'] 
            # for ind, row in dat2.iterrows():
            #     if ((row['close']-dat20_open)/dat20_open) >0.005:
            #         result[ind]+=1

            for ind, row in dat2.iterrows():
                up_=(row['close']-dat20_open)/dat20_open
                if up_ >up1:
                    num+=1
                    break
                if up_ <up2:
                    num2+=1
                    break
    conn.close()
    # for iii,a in enumerate(result):
    #     print(a,str(iii)+'天胜率:',a/df_no.shape[0])

    print(num,'{}内天至少有一天大于{}的概率:'.format(len,up1),num/df_no.shape[0])
    print(num2,'{}内天小于{}的概率:'.format(len,up2),num2/df_no.shape[0])
    print('没连续:',df_no.shape)
    print('有连续:',df_have.shape)

    col = []
    for i,t in enumerate(df_no.columns):
        col.append({ 'name': i, 'align': 'left', 'label': t, 'field': i, 'sortable': True, 
        'style': 'padding: 0px 0px', 'headerStyle':'padding: 0px 0px' })
    return JsonResponse({
        'col': col, 
        'da':df_no.values.tolist(),
        'code2':df_no['股票代码'].values.tolist(),
        'name2':df_no['股票简称'].values.tolist()
    })
    # return HttpResponse("You're voting on question %s." % question_id)