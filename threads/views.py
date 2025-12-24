from typing import Any
from django.db.models.base import Model as Model
from django.db.models.query import QuerySet
from django.forms import BaseModelForm
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.views import generic
from django.views.generic.edit import FormMixin
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import Http404
from threads.models import *
from threads.forms import *

# Create your views here.

class CategoryListView(generic.ListView):
    model = Category
    template_name = 'forum/category_list.html'
    context_object_name = 'categories'


class ThreadListView(LoginRequiredMixin, FormMixin, generic.ListView):
    model = Thread
    template_name = 'forum/thread_list.html'
    context_object_name = 'threads'
    paginate_by = 10
    form_class = ThreadCreateForm

    def get_success_url(self) -> str:
        return self.request.path

    def get_queryset(self) -> QuerySet[Any]:
        return Thread.objects.filter(category=self.category).order_by(self.order_by)
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['category'] = self.category
        context['ordering'] = self.order_by
        return context
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['author'] = self.request.user
        return kwargs
    
    def form_valid(self, form: Any) -> HttpResponse:
        form.instance.author = self.request.user
        form.instance.category = self.category
        return super().form_valid(form)
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.category: Category = get_object_or_404(Category, slug=self.kwargs['slug'])
        self.order_by = kwargs.pop('order_by')
        self.author = self.request.user
        if self.order_by not in ('-created_at', 'upvote_count'):
            raise Http404('Invalid ordering parameter!')
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request, *args, **kwargs): 
        form = self.get_form()
        if form.is_valid():
            return self.form_valid(form)
        else:
            self.object_list = self.get_queryset()
            return self.form_invalid(form)
    

class ThreadDetailView(LoginRequiredMixin, FormMixin, generic.DetailView):
    model = Thread
    template_name = 'forum/thread_detail.html'
    context_object_name = 'thread'
    form_class = ReplyCreateForm

    def get_success_url(self) -> str:
        return self.request.path

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['replies'] = self.object.replies.filter(is_deleted=False).select_related('author').order_by(self.order_by) # type: ignore
        return context
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['author'] = self.request.user
        return kwargs
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.order_by = kwargs.pop('order_by', None)
        self.author = self.request.user
        if self.order_by not in ('-created_at', 'upvote_count'):
            raise Http404('Invalid ordering parameter!')
        return super().dispatch(request, *args, **kwargs)
    
    def form_valid(self, form: Any) -> HttpResponse:
        form.instance.author = self.request.user
        form.instance.thread = self.object # type: ignore
        return super().form_valid(form)
    
    def post(self, request, *args, **kwargs): 
        form = self.get_form()
        if form.is_valid():
            return self.form_valid(form)
        else:
            self.object = self.get_object()
            return self.form_invalid(form)


class ReportCreateView(LoginRequiredMixin, generic.CreateView):
    model = Report
    form_class = ReportCreateForm
    template_name = 'forum/report_form.html'

    def get_success_url(self) -> str:
        return reverse_lazy('threads:thread_detail', kwargs={'pk': self.pk, 'order_by': self.order_by})

    def form_valid(self, form: BaseModelForm) -> HttpResponse:
        form.instance.reporter = self.request.user
        if self.type == 'thread':
            form.instance.thread = self.object
        elif self.type == 'reply':
            form.instance.reply = self.object
        return super().form_valid(form)
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.type = kwargs.pop('type', None)
        self.pk = kwargs.pop('pk', None)
        self.order_by = kwargs.pop('order_by', None)
        if self.order_by not in ('-created_at', 'upvote_count'):
            raise Http404('Invalid content parameters!')
        if self.type == 'thread':
            self.object = get_object_or_404(Thread, pk=self.pk)
            self.thread_pk = self.pk
        elif self.type == 'reply':
            self.object = get_object_or_404(Reply, pk=self.pk)
            self.thread_pk = self.object.thread.pk
        else:
            raise Http404('Invalid content parameters!')
        return super().dispatch(request, *args, **kwargs)
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['reporter'] = self.request.user
        return kwargs
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context =  super().get_context_data(**kwargs)
        context['type'] = self.type
        context['object'] = self.object
        context['back_url'] = self.get_success_url()
        return context
    
    
class UpvoteView(LoginRequiredMixin, generic.RedirectView):
    permanent = False

    def get_redirect_url(self, *args: Any, **kwargs: Any) -> str | None:
        return self.request.META.get('HTTP_REFERER', reverse_lazy('threads:category'))
    
    def post(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.obj.update_upvotes(self.user)
        return super().post(request, *args, **kwargs)
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.user = self.request.user
        self.type = kwargs.pop('type', None)
        self.pk = kwargs.pop('pk', None)
        if self.type == 'thread':
            self.obj = get_object_or_404(Thread, pk=self.pk)
        elif self.type == 'reply':
            self.obj = get_object_or_404(Reply, pk=self.pk)
        else:
            raise Http404('Invalid content parameters!')
        return super().dispatch(request, *args, **kwargs)
