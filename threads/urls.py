from django.urls import path
from threads.views import *

app_name = 'threads'
urlpatterns = [
    path('categories/', CategoryListView.as_view(), name='category_list'),
    path('categories/<slug:slug>/<str:order_by>/', ThreadListView.as_view(), name='thread_list'),
    path('thread/<int:pk>/<str:type>/<str:order_by>/report/', ReportCreateView.as_view(), name='report'),
    path('thread/<int:pk>/<str:type>/upvote/', UpvoteView.as_view(), name='upvote'),
    path('thread/<int:pk>/<str:order_by>/', ThreadDetailView.as_view(), name='thread_detail')
]
