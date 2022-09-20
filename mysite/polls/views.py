from django.http import HttpResponse, JsonResponse
import sqlite3
from .models import Question


def index(request):
    db_path = "./db.sqlite3"
    #连接数据库，如果不存在，则自动创建
    conn=sqlite3.connect(db_path)
    cur=conn.cursor()
    cu=cur.execute("select * from polls_east_yj_yg")
    d = cu.fetchmany(15)
    # print(d)
    cur.close()
    #断开数据库连接
    conn.close()
    col = [{ 'name': i, 'align': 'left', 'label': t[0], 'field': i, 'sortable': True } for i,t in enumerate(cu.description)]
    dict1 = [dict(zip(list(range(len(d[0]))),list(i))) for i in d]
    # print(col)
    return JsonResponse({'col': col, 'da':dict1})

def detail(request, question_id):
    return HttpResponse("You're looking at question %s." % question_id)

def results(request, question_id):
    response = "You're looking at the results of question %s."
    return HttpResponse(response % question_id)

def vote(request, question_id):
    return HttpResponse("You're voting on question %s." % question_id)