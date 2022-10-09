import pandas as pd
from . import tool_db

# 东财，输入代码获取股票k线数据 前复权, save == "y":  # 是否保存,fq=1前，=2后复权
def east_history_k_data(code, fq, save=''):
    """http://quote.eastmoney.com/concept/sh603233.html#fschart-k"""
    import requests
    import json
    net = r"""http://push2his.eastmoney.com/api/qt/stock/kline/get
            ?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&beg=0&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&rtntype=6&secid={}&klt=101&fqt={}&cb="""
    dragon_t = requests.get(net.format(code, fq)).text
    dragon_t = json.loads(dragon_t)
    # print(dragon_t['data'])
    dragon_t = dragon_t.get('data', '')
    if dragon_t:
        dragon_t = dragon_t.get('klines', '')
        dragon_t = [i.split(",") for i in dragon_t]
        """['2022-06-21', '27.53', '28.02', '28.05', '27.08', '59788', '166037892.00', '3.55', '2.60', '0.71', '0.63']
            date          open     close    high low volume money amplitude振幅 up_change涨跌幅 num_change涨跌额(元） turnover
        """
        arr1 = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'up_change', 'num_change',
                'turnover']
        dragon_t = pd.DataFrame(dragon_t, columns=arr1)
        # dragon_t[['date']] = dragon_t[['date']].apply(pd.to_datetime)
        dragon_t[['open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'up_change', 'num_change',
                    'turnover']] = dragon_t[['open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude',
                                            'up_change', 'num_change', 'turnover']].apply(pd.to_numeric)
        print(dragon_t.head())
        # print(dragon_t.dtypes)
        if save == "y":  # 是否保存
            conn,cur=tool_db.get_conn_cur()
            dragon_t.to_sql('east'+code+'_'+str(fq), con=conn, if_exists='replace', index=False)
            conn.close()
