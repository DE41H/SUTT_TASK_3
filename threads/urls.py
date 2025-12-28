from django.urls import path
from threads.views import (
    CategoryListView,
    ThreadListView,
    ThreadDetailView,
    ThreadCreateView,
    ReportCreateView,
    ReportListView,
    UpvoteView,
    DeleteView,
    LockView,
    ThreadEditView,
    ReplyEditView
)

app_name = 'threads'
urlpatterns = [
    path('categories/', CategoryListView.as_view(), name='category_list'),
    path('categories/<slug:slug>/<str:order_by>/', ThreadListView.as_view(), name='thread_list'),
    path('view/<int:pk>/<str:order_by>/', ThreadDetailView.as_view(), name='thread_detail'),
    path('create/<int:pk>/', ThreadCreateView.as_view(), name='thread_create'),
    path('edit/<int:pk>/thread/', ThreadEditView.as_view(), name='thread_edit'),
    path('edit/<int:pk>/reply/', ReplyEditView.as_view(), name='reply_edit'),
    path('reports/create/<int:pk>/<str:type>', ReportCreateView.as_view(), name='report_create'),
    path('reports/list/', ReportListView.as_view(), name='report_list'),
    path('upvote/<int:pk>/<str:type>', UpvoteView.as_view(), name='upvote'),
    path('delete/<int:pk>/<str:type>', DeleteView.as_view(), name='delete'),
    path('lock/<int:pk>/', LockView.as_view(), name='lock')
]
