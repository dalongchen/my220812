from time import time

db_path = r"D:\myzq\axzq\T0002\stock_load\my_stock\my_stock.db"  # 数据库路径
# 银行500亿以上
bank_li = """sh601398,sz000001,sz002142,sh600000,sh600015,sh600016,sh600036,sh600919,sh600926,sh601009,sh601166,sh601169,sh601229,sh601288,sh601328,sh601658,sh601818,sh601838,sh601916,sh601939,sh601988,sh601998"""

def time_show(func):
    def new_func(*arg, **kw):
        t1 = time()
        res = func(*arg, **kw)
        t2 = time()
        print(f"{func.__name__: >10} : {t2-t1:.6f} sec")  # 前加f表示字符串内支持大括号内的python表达式
        return res
    return new_func

def add_sh(code, f=""):  # big="baostock"加(sh. or sz.)code加(sh or sz) or (SZ or SH)
    if f == "":
        if code.startswith("0") or code.startswith("3") or code.startswith("2"):
            code = "sz" + code
        elif code.startswith("5") or code.startswith("6") or code.startswith("9"):
            code = "sh" + code
        else:
            print("err1", code)
    elif f == "01":
        if code.startswith("0"):
            code = "sz" + code[1:]
        elif code.startswith("1"):
            code = "sh" + code[1:]
        else:
            print("err2", code)
    elif f == "baostock":
        if code.startswith("0") or code.startswith("3") or code.startswith("2"):
            code = "sz." + code
        elif code.startswith("5") or code.startswith("6") or code.startswith("9"):
            code = "sh." + code
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



