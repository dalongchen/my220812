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

def results(request, question_id):
    inp=request.GET.get('inp',default='110')
    inp2 = tools.add_sh(inp, big='east.')
    print(question_id, inp2)  
    conn,cur=tool_db.get_conn_cur()
    """'date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'up_change', 
    'num_change', 'turnover'"""
    sql = r"""select date,close,open,low,high,volume from '{}' where volume != 0""".format('east'+inp2+'_2')
    try:
        dat2 = pd.read_sql(sql, conn )
    except Exception as e:
        # print(e)
        if 'no such table' in str(e):
            tool_east.east_history_k_data(inp2,2,save='y')  # fq=1前，=2后复权
            dat2 = pd.read_sql(sql, conn )
        else:
            raise e
    # dat3 = tools.stockk2(dat2)   # 后复权
    dat1_ = dat2.iloc[:,1:]
    dat1_pre = dat1_.values  # 未改变前的值
    dat1_.insert(4,'i',dat1_.index.tolist()) 
    dat1_['max'] = dat1_.apply(lambda x: 1 if x['close'] > x['open'] else -1, axis=1)
    

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
        # import datetime as dt
        sql_high = r"""select date,high from '{}' where date='{}'"""
        def datacheck(data):
            dat_high = pd.read_sql(sql_high.format('east'+inp2+'_2',data), conn )
            if not dat_high.empty:
                # return dat_high
                return dat_high.values[0]
            else:
                sql_high2 = r"""select date,high from '{}' where date>'{}'"""
                dat_high = pd.read_sql(sql_high2.format('east'+inp2+'_2',data), conn )
                if not dat_high.empty:
                    # return dat_high.head(1)
                    return dat_high.head(1).values[0]
                else:
                    raise
                    return  ['','']
                    # return  pd.DataFrame({'date':[],'high':[]})
        #     # day = dt.datetime.strptime(data,'%Y-%m-%d') +dt.timedelta(days=i)
        #     day = pd.to_datetime(data) + dt.timedelta(days=i)
        #     print(day.strftime("%Y-%m-%d"))
        dd=dat_yj_yg['公告日期'].apply(datacheck)
        # print(list(dd))
        dat_yj_yg[['显示日期','最高价']]=list(dd)
        dat_yj_yg[['code']]=inp
        # print(dat_yj_yg[['显示日期','最高价']])
        sql_s = r"UPDATE polls_east_yj_yg SET 显示日期=(?),最高价=(?) where  公告日期=(?) and 股票代码=(?)"
        # print(sql_s)
        data_s = dat_yj_yg[['显示日期', '最高价', '公告日期','code']]
        # print(data_s.values)
        cur.executemany(sql_s, data_s.values)
        # print(cur.rowcount)
        conn.commit()
        dat_yj_yg = pd.read_sql(sql_yj_yg,conn)

    conn.close()
    dat_yj_yg = dat_yj_yg[['显示日期', '最高价', '公告日期','预告类型','预测数值','业绩变动','业绩变动原因']]
    # add_col_sql="""alter table polls_east_yj_yg add 最高价 INTEGER"""
    # tools.conn_db_table_update(sql=add_col_sql)
    
    return JsonResponse({
        'categoryData': dat2.date.values.tolist(),
        'values':dat1_pre.tolist(),
        'volumes':dat1_.iloc[:,4:].values.tolist(),
        'dat_yj_yg':dat_yj_yg.values.tolist()
    })

@tools.time_show
def vote(request, question_id):
    sql ="""SELECT 股票代码,股票简称,预测指标,业绩变动,预测数值,业绩变动幅度 as 幅度,
    业绩变动原因,预告类型 as 类型,上年同期值,公告日期  FROM polls_east_yj_yg as p WHERE
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
    df = df.head(20)

    save=''
    if save == "y":  # 是否保存
        conn,cur=tool_db.get_conn_cur()
        df.to_sql('east_pre_add_temporary', con=conn, if_exists='replace', index=False)

    df_no = df.copy(deep=True)  # 前一季度没有
    df_have = df.copy(deep=True)  # 前一季度有

    ii=0
    # 获取股价sql
    sql_high = r"""select date,open,close,high,low from '{}' where date>='{}' LIMIT 1"""
    # 获取前一季度也预告的股票sql
    sql_continuity ="""SELECT * FROM polls_east_yj_yg as p WHERE
    p."股票代码" = '{}' AND
    p."业绩变动" LIKE '%2020年1-3%' and (
    p."预告类型" LIKE '预增' or
    p."预告类型" LIKE '略增' or
    p."预告类型" LIKE '扭亏' )
    """
    for i,t in df.iterrows():
        dat2 = pd.read_sql(sql_continuity.format(t['股票代码']), conn )
        if dat2.shape[0]!=0:
            df_no.drop([i],inplace=True)
        else:
            df_have.drop([i],inplace=True)

            # 计算股价胜率
            inp2 = tools.add_sh(t['股票代码'], big='east.')
            try:
                dat2 = pd.read_sql(sql_high.format('east'+inp2+'_2',t['公告日期']), conn )
            except Exception as e:
                # print(e)
                if 'no such table' in str(e):
                    print('new table',inp2)
                    tool_east.east_history_k_data(inp2,2,save='y')  # fq=1前，=2后复权
                    time.sleep(1.3)
                    dat2 = pd.read_sql(sql_high.format('east'+inp2+'_2',t['公告日期']), conn )
                else:
                    raise e
            upchange = (dat2['close']-dat2['open'])/dat2['open']
            # print(round(upchange[0],4))
            if upchange[0]>0.005:
                ii+=1
    conn.close()

    print(ii,'胜率:',ii/df.shape[0])
        
    print(df_no)
    print(df_have)

    col = []
    for i,t in enumerate(df_no.columns):
        col.append({ 'name': i, 'align': 'left', 'label': t, 'field': i, 'sortable': True, 
        'style': 'padding: 0px 0px', 'headerStyle':'padding: 0px 0px' })
    return JsonResponse({'col': col, 'da':df_no.values.tolist(),'code2':df_no['股票代码'].values.tolist()})
    # return JsonResponse({'col': col, 'da':df_have.values.tolist(),'code2':df_have['股票代码'].values.tolist()})
    # return HttpResponse("You're voting on question %s." % question_id)