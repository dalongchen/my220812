import pandas as pd
from . import tool_db


def stockk0(inp):  # 不复权
    conn,cur=tool_db.get_conn_cur()
    """CREATE TABLE "sh600000" (
	"日期"	TEXT,
	"收盘价"	REAL,
	"最高价"	REAL,
	"最低价"	REAL,
	"开盘价"	REAL,
	"前收盘"	REAL,
	"涨跌额"	REAL,
	"涨跌幅"	REAL,
	"换手率"	REAL,
	"成交量"	REAL,
	"成交金额"	REAL,
	"总市值"	REAL,
	"流通市值"	REAL
    )"""
    sql = r"""select 日期,收盘价,开盘价,最低价,最高价,成交量 from {} where 成交量 != 0""".format(inp)
    dat2 = pd.read_sql(sql, conn )
    conn.close()
    dat2 = dat2.head(3) 
    # print(dat2)
    dat1_ = dat2.iloc[:,1:]
    dat1_pre = dat1_.values  # 未改变前的值
    dat1_.insert(4,'i',dat1_.index.tolist()) 
    dat1_['max'] = dat1_.apply(lambda x: 1 if x['收盘价'] > x['开盘价'] else -1, axis=1)
    # print(dat1_.iloc[:,4:].values)
    dat3 = {'categoryData': dat2.日期.values.tolist(),'values':dat1_pre.tolist(), 'volumes':dat1_.iloc[:,4:].values.tolist()}
    return dat3

def stockk2(dat2):  # 后复权
    # inp = add_sh(inp,big='east.')
    dat1_ = dat2.iloc[:,1:]
    dat1_pre = dat1_.values  # 未改变前的值
    dat1_.insert(4,'i',dat1_.index.tolist()) 
    dat1_['max'] = dat1_.apply(lambda x: 1 if x['close'] > x['open'] else -1, axis=1)
    dat3 = {'categoryData': dat2.date.values.tolist(),'values':dat1_pre.tolist(), 'volumes':dat1_.iloc[:,4:].values.tolist()}
    return dat3

def add_sh(code, big=""):  # big="baostock"加(sh. or sz.)code加(sh or sz) or (SZ or SH)
    if big == "":
        if code.startswith("0") or code.startswith("3") or code.startswith("2"):
            code = "sz" + code
        elif code.startswith("5") or code.startswith("6") or code.startswith("9"):
            code = "sh" + code
        else:
            print("err1", code)
    elif big == "baostock":
        if code.startswith("0") or code.startswith("3") or code.startswith("2"):
            code = "sz." + code
        elif code.startswith("5") or code.startswith("6") or code.startswith("9"):
            code = "sh." + code
        else:
            print("err2", code)
    elif big == "east.":
        if code.startswith("0") or code.startswith("3") or code.startswith("2"):
            code = "0." + code
        elif code.startswith("5") or code.startswith("6") or code.startswith("9"):
            code = "1." + code
        else:
            print("err2", code)
    else:
        if code.startswith("0") or code.startswith("3") or code.startswith("2"):
            code = "SZ" + code
        elif code.startswith("5") or code.startswith("6") or code.startswith("9"):
            code = "SH" + code
        else:
            print("err3", code)
    return code