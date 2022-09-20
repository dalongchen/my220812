from time import time
ax_tdx = r"""D:\myzq\axzq\T0002\blocknew"""
db_path = "/data/home/chen/Documents/my_stock.db"
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



