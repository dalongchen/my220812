from time import time
db_path = "/home/chen/Documents/my220812/stock/my_stock_test.db"
db_path_poll = "/home/chen/Documents/my220812/mysite/polls/polls_db.sqlite3"
db_path_my_school_test = "/home/chen/Documents/my220812/stock/my_school_test/my_school_test.sqlite3"

def time_show(func):
    def new_func(*arg, **kw):
        t1 = time()
        res = func(*arg, **kw)
        t2 = time()
        print(f"{func.__name__: >10} : {t2-t1:.6f} sec")  # 前加f表示字符串内支持大括号内的python表达式
        return res
    return new_func



