import sqlite3
import global_variable as gl_v

@gl_v.time_show
def history_volatility():
    #连接数据库，如果不存在，则自动创建
    conn=sqlite3.connect(gl_v.db_path)
    #创建游标
    cur=conn.cursor()
    cu=cur.execute("select 日期,涨跌幅 from 'df601398hfq'")
    print(cu.fetchone())
    #关闭游标
    cur.close()
    #断开数据库连接
    conn.close()
       
history_volatility()