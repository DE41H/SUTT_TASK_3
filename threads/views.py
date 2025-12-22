from typing import Any
from django.db.models.base import Model as Model
from django.db.models.query import QuerySet
from django.forms import BaseModelForm
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.views import generic
from django.views.generic.edit import FormMixin
from django.http import Http404
from threads.models import *
from threads.forms import *

# Create your views here.

class CategoryListView(generic.ListView):
    model = Category
    template_name = 'forum/category_list.html'
    context_object_name = 'categories'


class ThreadListView(FormMixin, generic.ListView):
    model = Thread
    template_name = 'forum/thread_list.html'
    context_object_name = 'threads'
    paginate_by = 10
    form_class = ThreadCreateForm
    success_url = reverse_lazy('threads:thread_list')

    def get_queryset(self) -> QuerySet[Any]:
        self.category: Category = get_object_or_404(Category, slug=self.kwargs['slug'])
        self.order_by = self.kwargs['order_by']
        if self.order_by not in ('-created_at', 'upvote_count'):
            raise Http404('Invalid ordering parameter!')
        return Thread.objects.filter(category=self.category).order_by(self.order_by)
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['category'] = self.category
        context['ordering'] = self.order_by
        return context
    
    def form_valid(self, form: Any) -> HttpResponse:
        Thread.objects.create(
            title=form.instance.title,
            raw_content=form.instance.raw_content,
        )
        return super().form_valid(form)
    

class ThreadDetailView(generic.DetailView):
    model = Thread
    template_name = 'forum/thread_detail.html'
    context_object_name = 'thread'

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['replies'] = self.object.replies.select_related('author').order_by('created_at') # type: ignore
        context['reply_form'] = ReplyCreateForm
        return context


class ReportCreateView(generic.CreateView):
    model = Report
    form_class = ReportCreateForm
    template_name = 'forum/report_form.html'
    success_url = reverse_lazy('threads:category_list')

    def form_valid(self, form: BaseModelForm) -> HttpResponse:
        form.instance.reporter = self.request.user
        if self.type == 'thread':
            form.instance.thread = self.object
        elif self.type == 'reply':
            form.instance.reply = self.object
        return super().form_valid(form)
    
    def dispatch(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.type = self.kwargs['type']
        self.pk = self.kwargs['pk']
        if self.type == 'thread':
            self.object = get_object_or_404(Thread, pk=self.pk)
        elif self.type == 'reply':
            self.object = get_object_or_404(Reply, pk=self.pk)
        else:
            raise Http404('Invalid content parameters!')
        return super().dispatch(request, *args, **kwargs)
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['reporter'] = self.request.user
        return kwargs
