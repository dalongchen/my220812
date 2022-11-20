from django.urls import path

from . import views

urlpatterns = [
    # ex: /polls/
    path('', views.index, name='index'),
    # ex: /polls/5/
    path('<int:question_id>/', views.detail, name='detail'),
    # ex: /polls/5/results/
    path('<int:question_id>/results/', views.results, name='results'),
    # ex: /polls/5/stockStandardk/
    path('<int:question_id>/stockStandardk/', views.stock_standard_k,
         name='stock_standard_k'),
    # ex: /polls/5/vote/
    path('<int:question_id>/vote/', views.vote, name='vote'),
    # ex: /polls/stockYjbbEm/
    path('stockYjbbEm/', views.stock_yjbb_em, name='stock_yjbb_em'),
    # ex: /polls/zhangTing/
    path('zhangTing/', views.zhang_ting, name='zhang_ting'),
    # ex: /polls/update_day_k/
    path('update_day_k/', views.update_day_k, name='update_day_k'),
]
