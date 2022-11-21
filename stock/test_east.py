import global_variable as gl_v


# 东财，获取全部股票配股数据, save == "y":  # 是否保存
def east_history_peigu_data(save=''):
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
        conn, cur = gl_v.get_conn_cur()
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
                    if i == 1:
                        data_t.to_sql('east_history_peigu', con=conn,
                                      if_exists='replace', index=False)
                    else:
                        data_t.to_sql('east_history_peigu', con=conn,
                                      if_exists='append', index=False)
            time.sleep(0.7)
        conn.close()


east_history_peigu_data(save='')
