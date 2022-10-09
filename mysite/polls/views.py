from django.http import HttpResponse, JsonResponse
from .mytool import tool_db,tools,tool_east
import pandas as pd


def index(request):
    sql ="""SELECT * FROM polls_east_yj_yg as p WHERE
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
    d = cur.execute(sql).fetchall()
    cu_desc = cur.description
    col = []
    for i,t in enumerate(cu_desc):
        tt = t[0]
        if tt=='业绩变动幅度':
            tt='幅度'
        if tt=='预告类型':
            tt='类型'
        col.append({ 'name': i, 'align': 'left', 'label': tt, 'field': i, 'sortable': True, 
        'style': 'padding: 0px 0px', 'headerStyle':'padding: 0px 0px' })

    dict1 = [dict(zip(list(range(len(d[0]))),list(ii))) for ii in d]
    conn.close()
    return JsonResponse({'col': col, 'da':dict1})


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
        print(e)
        if 'no such table' in str(e):
            tool_east.east_history_k_data(inp2,2,save='y')  # fq=1前，=2后复权
            dat2 = pd.read_sql(sql, conn )
        else:
            raise e
    dat3 = tools.stockk2(dat2)   # 后复权

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
    
    sql_yj_yg = r"""select * from polls_east_yj_yg where 股票代码 = '{}' GROUP BY 公告日期""".format(inp)
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
    conn.close()
    dat_yj_yg = dat_yj_yg[['显示日期', '最高价', '公告日期','预告类型','预测数值','业绩变动','业绩变动原因']]
    # add_col_sql="""alter table polls_east_yj_yg add 最高价 INTEGER"""
    # tools.conn_db_table_update(sql=add_col_sql)
    # print(dat_yj_yg)
    return JsonResponse({'dat':dat3,'dat_yj_yg':dat_yj_yg.values.tolist()})


def vote(request, question_id):
    return HttpResponse("You're voting on question %s." % question_id)