from django.urls import path
from threads.views import *

app_name = 'threads'
urlpatterns = [
    path('categories/', CategoryListView.as_view(), name='category_list'),
    path('categories/<slug:slug>/<str:order_by>/', ThreadListView.as_view(), name='category_detail'),
    path('thread/<int:pk>/<str:type>/report/', ReportCreateView.as_view(), name='report'),
    path('thread/<int:pk>/', ThreadDetailView.as_view())
]
