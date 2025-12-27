from typing import Any
from django.db.models.base import Model as Model
from django.db.models.query import QuerySet
from django.forms import BaseModelForm
from django.http import HttpRequest, HttpResponse, Http404
from django.utils.http import url_has_allowed_host_and_scheme
from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.views import generic
from django.views.generic.edit import FormMixin
from django.contrib.auth.mixins import LoginRequiredMixin
from threads.models import *
from threads.forms import *
from threads.utils import *

# Create your views here.

class CategoryListView(generic.ListView):
    model = Category
    template_name = 'threads/category_list.html'
    context_object_name = 'categories'


class ThreadListView(generic.ListView):
    model = Thread
    template_name = 'threads/thread_list.html'
    context_object_name = 'threads'
    paginate_by = 10

    def get_queryset(self) -> QuerySet[Any]:
        if self.query is None or self.query == '':
            return Thread.objects.filter(category=self.category).order_by(self.order_by)
        return fuzzy_search(self.query).filter(category=self.category).order_by(self.order_by)
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['category'] = self.category
        context['query'] = self.query
        return context
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.query = self.request.GET.get('q')
        self.category: Category = get_object_or_404(Category, slug=self.kwargs['slug'])
        self.order_by = kwargs.get('order_by')
        if self.order_by not in ('-created_at', '-upvote_count'):
            raise Http404('Invalid content parameters!')
        return super().dispatch(request, *args, **kwargs)


class ThreadCreateView(LoginRequiredMixin, generic.CreateView):
    model = Thread
    form_class = ThreadCreateForm
    template_name = 'threads/thread_create.html'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts={self.request.get_host()}):
            return next
        return reverse_lazy('threads:thread_list', kwargs={'slug': self.category.slug, 'order_by': '-created_at'})
    
    def form_valid(self, form: BaseModelForm) -> HttpResponse:
        form.instance.author = self.author
        form.instance.category = self.category
        return super().form_valid(form)
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['author'] = self.author
        return kwargs
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['category'] = self.category
        return context
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.pk = kwargs.get('pk')
        self.author = self.request.user
        self.category = get_object_or_404(Category, pk=self.pk)
        return super().dispatch(request, *args, **kwargs)


class ThreadDetailView(LoginRequiredMixin, FormMixin, generic.DetailView):
    model = Thread
    template_name = 'threads/thread_detail.html'
    context_object_name = 'thread'
    form_class = ReplyCreateForm

    def get_success_url(self) -> str:
        return self.request.path

    def post(self, request, *args, **kwargs): 
        form = self.get_form()
        if form.is_valid():
            return self.form_valid(form)
        else:
            self.object = self.get_object()
            return self.form_invalid(form)
    
    def form_valid(self, form: Any) -> HttpResponse:
        reply = form.save(commit=False)
        reply.author = self.author
        reply.thread = self.get_object()
        reply.save()
        return super().form_valid(form)

    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['author'] = self.author
        return kwargs

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['replies'] = self.get_object().replies.filter(is_deleted=False).select_related('author').order_by(self.order_by) # type: ignore
        return context
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.order_by = kwargs.get('order_by')
        self.author = self.request.user
        if self.order_by not in ('-created_at', '-upvote_count'):
            raise Http404('Invalid ordering parameter!')
        return super().dispatch(request, *args, **kwargs)


class ReportCreateView(LoginRequiredMixin, generic.CreateView):
    model = Report
    form_class = ReportCreateForm
    template_name = 'threads/report_create.html'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts={self.request.get_host()}):
            return next
        return reverse_lazy('threads:thread_detail', kwargs={'pk': self.thread_pk, 'order_by': '-created_at'})
    
    def form_valid(self, form: BaseModelForm) -> HttpResponse:
        form.instance.reporter = self.request.user
        if self.type == 'thread':
            print('thread')
            form.instance.thread = self.obj
        elif self.type == 'reply':
            print('reply')
            form.instance.reply = self.obj
        return super().form_valid(form)
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['reporter'] = self.request.user
        return kwargs
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context =  super().get_context_data(**kwargs)
        context['type'] = self.type
        context['object'] = self.obj
        context['back_url'] = self.get_success_url()
        return context
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.type = kwargs.get('type')
        self.pk = kwargs.get('pk')
        if self.type == 'thread':
            self.obj = get_object_or_404(Thread, pk=self.pk)
            self.thread_pk = self.pk
        elif self.type == 'reply':
            self.obj = get_object_or_404(Reply, pk=self.pk)
            self.thread_pk = self.obj.thread.pk
        else:
            raise Http404('Invalid content parameters!')
        return super().dispatch(request, *args, **kwargs)


class UpvoteView(LoginRequiredMixin, generic.RedirectView):
    permanent = False

    def get_redirect_url(self, *args: Any, **kwargs: Any) -> str | None:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts={self.request.get_host()}):
            return next
        return reverse_lazy('threads:thread_list', kwargs={'pk': self.thread_pk, 'order_by': '-created_at'})
    
    def post(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.object.update_upvotes(self.user)
        return super().post(request, *args, **kwargs)
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.user = self.request.user
        self.type = kwargs.get('type')
        self.pk = kwargs.get('pk')
        if self.type == 'thread':
            self.object = get_object_or_404(Thread, pk=self.pk)
            self.thread_pk = self.pk
        elif self.type == 'reply':
            self.object = get_object_or_404(Reply, pk=self.pk)
            self.thread_pk = self.object.thread.pk
        else:
            raise Http404('Invalid content parameters!')
        return super().dispatch(request, *args, **kwargs)
