

# 自己写,东财，输入代码获取股票k线数据 前复权, save == "y":  # 是否保存,fq=1前，=2后复权
def east_history_k_data(code, fq, save=''):
    """http://quote.eastmoney.com/concept/sh603233.html#fschart-k"""
    import pandas as pd
    from . import tool_db
    import requests
    import json

    net = r"""http://push2his.eastmoney.com/api/qt/stock/kline/get
            ?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&
            fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&
            beg=0&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&
            rtntype=6&secid={}&klt=101&fqt={}&cb="""
    dragon_t = requests.get(net.format(code, fq)).text
    dragon_t = json.loads(dragon_t)
    dragon_t = dragon_t.get('data', '')
    if dragon_t:
        dragon_t = dragon_t.get('klines', '')
        dragon_t = [i.split(",") for i in dragon_t]
        arr1 = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount',
                'amplitude', 'up_change', 'num_change', 'turnover']
        dragon_t = pd.DataFrame(dragon_t, columns=arr1)
        # dragon_t[['date']] = dragon_t[['date']].apply(pd.to_datetime)
        dragon_t[['open',
                  'close',
                  'high',
                  'low',
                  'volume',
                  'amount',
                  'amplitude',
                  'up_change',
                  'num_change',
                  'turnover']] = dragon_t[['open',
                                           'close',
                                           'high',
                                           'low',
                                           'volume',
                                           'amount',
                                           'amplitude',
                                           'up_change',
                                           'num_change',
                                           'turnover']].apply(pd.to_numeric)
        # print(dragon_t.head())
        # print(dragon_t.dtypes)
        if save == "y":  # 是否保存
            conn, cur = tool_db.get_conn_cur()
            dragon_t.to_sql('east'+code+'_'+str(fq), con=conn,
                            if_exists='replace', index=False)
            conn.close()


# 东财，获取全部股票配股数据, save == "y":  # 是否保存
def east_history_peigu_data(save, conn):
    """https://data.eastmoney.com/xg/pg/"""
    import pandas as pd
    import requests
    import json
    import time

    net = r"""https://datacenter-web.eastmoney.com/api/data/v1/get?callback=&sortColumns=EQUITY_RECORD_DATE&sortTypes=-1&pageSize=500&pageNumber={}&reportName=RPT_IPO_ALLOTMENT&columns=ALL&quoteColumns=f2~01~SECURITY_CODE~NEW_PRICE&quoteType=0&source=WEB&client=WEB"""
    result2 = json.loads(requests.get(net.format(1)).text)
    result2 = result2.get('result', '')
    # print(result2)
    pages = result2.get('pages', '')
    print(pages)
    if pages:
        # conn, cur = gl_v.get_conn_cur()
        for i in range(1, pages+1):
            print(i)
            if i > 1:
                # break
                result2 = json.loads(requests.get(net.format(i)).text)
                result2 = result2.get('result', '')
            data_t = result2.get('data', '')
            if data_t:
                data_t = pd.DataFrame(data_t)
                # print(data_t)
                data_t.rename(columns={
                    'SECURITY_CODE': '代码',
                    'SECURITY_NAME_ABBR': '名称',
                    'PLACING_RATIO': '配股比',
                    'ISSUE_PRICE': '配股价',
                    'TOTAL_SHARES_BEFORE': '配前股本w',
                    'ISSUE_NUM': '配股数量',
                    'TOTAL_SHARES_AFTER': '配后股本w',
                    'EQUITY_RECORD_DATE': '登记日',
                    'EX_DIVIDEND_DATE': '除权日',
                }, inplace=True)
                # print(data_t)
                data_t = data_t[[
                    '代码',
                    '名称',
                    '配股比',
                    '配股价',
                    '配前股本w',
                    '配股数量',
                    '配后股本w',
                    '登记日',
                    '除权日',
                ]]
                # print(data_t)
                data_t['登记日'] = data_t['登记日'].str[:10]
                data_t['除权日'] = data_t['除权日'].str[:10]
                print(data_t)
                if save == "y":  # 是否保存
                    if i == 1:  # 第一页时重新建表,不是第一页就add数据
                        data_t.to_sql('east_history_peigu', con=conn,
                                      if_exists='replace', index=False)
                    else:
                        data_t.to_sql('east_history_peigu', con=conn,
                                      if_exists='append', index=False)
            time.sleep(0.5)
        # conn.close()
