from typing import Any
from django.conf import settings
from django.db.models import Count
from django.db.models.base import Model as Model
from django.db.models.query import QuerySet
from django.forms import BaseModelForm
from django.http import HttpRequest, HttpResponse, Http404
from django.utils.functional import cached_property
from django.utils.http import url_has_allowed_host_and_scheme
from django.shortcuts import get_object_or_404
from django.urls import reverse_lazy
from django.views import generic
from django.views.generic.edit import FormMixin
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from threads.models import Category, Thread, Reply, Report, Tag
from threads.forms import ReplyCreateForm, ReportCreateForm, ThreadCreateForm, TagCreateForm
from threads.utils import generate_random_color

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
        if self.query and self.query != '':
            qs = Thread.fuzzy_search(self.query)
        else:
            qs = Thread.objects.all()
        if self.filters:
            qs = qs.filter(category=self.category, is_deleted=False, tags__in=self.selected_tags).distinct().order_by(self.order_by)
        else:
            qs = qs.filter(category=self.category, is_deleted=False).order_by(self.order_by)
        return qs.prefetch_related('tags').select_related('author', 'category')
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['category'] = self.category
        context['query'] = self.query
        context['selected'] = self.selected_tags
        context['tags'] = Tag.objects.annotate(threads=Count('tagged')).filter(threads__gte=1).order_by('-threads')
        return context

    @cached_property
    def category(self):
        return get_object_or_404(Category, slug=self.kwargs.get('slug'))
    
    @cached_property
    def order_by(self):
        order_by = self.kwargs.get('order_by')
        if order_by not in ('-created_at', '-upvote_count'):
            raise Http404('Invalid content parameters!')
        return order_by
    
    @cached_property
    def filters(self):
        return self.request.GET.get('f')
    
    @cached_property
    def selected_tags(self):
        if self.filters:
            selected = [i for i in self.filters.split(',') if i]
            selected_tags = tuple(Tag.objects.filter(name__in=selected))
        else:
            selected_tags = tuple()
        return selected_tags
    
    @cached_property
    def query(self):
        return self.request.GET.get('q')


class ThreadCreateView(LoginRequiredMixin, generic.CreateView):
    model = Thread
    form_class = ThreadCreateForm
    template_name = 'threads/thread_create.html'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
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
    
    @cached_property
    def author(self):
        return self.request.user
    
    @cached_property
    def category(self):
        pk = self.kwargs.get('pk')
        return get_object_or_404(Category, pk=pk)
    

class ThreadEditView(LoginRequiredMixin, UserPassesTestMixin, generic.UpdateView):
    model = Thread
    fields = ['title', 'raw_content', 'tags', 'tagged_courses', 'tagged_documents']
    template_name = 'threads/thread_edit.html'
    context_object_name = 'thread'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_detail', kwargs={'pk': self.get_object().pk, 'order_by': '-created_at'}) # type: ignore

    def test_func(self) -> bool | None:
        return self.get_object().author == self.request.user # type: ignore
    
class ReplyEditView(LoginRequiredMixin, UserPassesTestMixin, generic.UpdateView):
    model = Reply
    fields = ['raw_content']
    template_name = 'threads/reply_edit.html'
    context_object_name = 'reply'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_detail', kwargs={'pk': self.get_object().thread.pk, 'order_by': '-created_at'}) # type: ignore

    def test_func(self) -> bool | None:
        return self.get_object().author == self.request.user # type: ignore


class ThreadDetailView(LoginRequiredMixin, FormMixin, generic.DetailView):
    model = Thread
    template_name = 'threads/thread_detail.html'
    context_object_name = 'thread'
    form_class = ReplyCreateForm

    def get_success_url(self) -> str:
        return self.request.path

    def post(self, request, *args, **kwargs): 
        self.object = self.get_object()
        form = self.get_form()
        if self.object.is_locked: # type: ignore
            form.add_error(None, 'This thread is locked!')
            return self.form_invalid(form)
        if form.is_valid():
            return self.form_valid(form)
        else:
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
    
    def get_queryset(self) -> QuerySet[Any]:
        return super().get_queryset().select_related('author', 'category').prefetch_related('tags').filter(is_deleted=False)
    
    @cached_property
    def author(self):
        return self.request.user
    
    @cached_property
    def order_by(self):
        order_by = self.kwargs.get('order_by')
        if order_by not in ('-created_at', '-upvote_count'):
            raise Http404('Invalid ordering parameter!')
        return order_by


class ReportCreateView(LoginRequiredMixin, generic.CreateView):
    model = Report
    form_class = ReportCreateForm
    template_name = 'threads/report_create.html'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_detail', kwargs={'pk': self.thread_pk, 'order_by': '-created_at'})
    
    def form_valid(self, form: BaseModelForm) -> HttpResponse:
        form.instance.reporter = self.request.user
        match self.type:
            case 'thread':
                form.instance.thread = self.obj
            case 'reply':
                form.instance.reply = self.obj
        return super().form_valid(form)
    
    def get_form_kwargs(self) -> dict[str, Any]:
        kwargs = super().get_form_kwargs()
        kwargs['reporter'] = self.user
        return kwargs
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context =  super().get_context_data(**kwargs)
        context['type'] = self.type
        context['object'] = self.obj
        context['back_url'] = self.get_success_url()
        return context
    
    @cached_property
    def user(self):
        return self.request.user
    
    @cached_property
    def type(self):
        return self.kwargs.get('type')
    
    @cached_property
    def obj(self):
        pk = self.kwargs.get('pk')
        match self.type:
            case 'thread':
                obj = get_object_or_404(Thread, pk=pk)
            case 'reply':
                obj = get_object_or_404(Reply, pk=pk)
            case _:
                raise Http404('Invalid content parameters!')
        return obj

    @cached_property
    def thread_pk(self):
        match self.type:
            case 'thread':
                thread_pk = self.obj.pk
            case 'reply':
                thread_pk = self.obj.thread.pk # type: ignore
            case _:
                raise Http404('Invalid content parameters!')
        return thread_pk


class TagCreateView(LoginRequiredMixin, generic.FormView):
    model = Tag
    form_class = TagCreateForm
    template_name = 'threads/tag_create.html'

    def get_success_url(self) -> str:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:category_list')
    
    def form_valid(self, form: BaseModelForm) -> HttpResponse:
        tags = form.cleaned_data.get('tags')
        if tags:
            objects = [Tag(name=f'#{name.lower()}', color=generate_random_color()) for name in tags.split(' ') if name]
            Tag.objects.bulk_create(objects, ignore_conflicts=True)
        return super().form_valid(form)


class ReportListView(LoginRequiredMixin, UserPassesTestMixin, generic.ListView):
    model = Report
    template_name = 'threads/report_list.html'
    context_object_name = 'reports'
    paginate_by = 10

    def get_queryset(self) -> QuerySet[Any]:
        return Report.objects.select_related('reporter', 'reply', 'thread').order_by('status', '-created_at')
    
    def test_func(self) -> bool | None:
        return self.request.user.is_staff
    

class ReportUpdateStatusView(LoginRequiredMixin, UserPassesTestMixin, generic.RedirectView):
    permanent = False

    def get_redirect_url(self, *args: Any, **kwargs: Any) -> str | None:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_list', kwargs={'slug': self.slug, 'order_by': '-created_at'})
    
    def post(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.object.update_status()
        return super().post(request, *args, **kwargs)
    
    def test_func(self) -> bool | None:
        return self.request.user.is_staff
    
    @cached_property
    def object(self):
        pk = self.kwargs.get('pk')
        return get_object_or_404(Report, pk=pk)
    
    @cached_property
    def slug(self):
        if self.object.thread:
            slug = self.object.thread.category.slug
        elif self.object.reply:
            slug = self.object.reply.thread.category.slug
        else:
            raise Http404('Invalid content parameters!')
        return slug


class UpvoteView(LoginRequiredMixin, generic.RedirectView):
    permanent = False

    def get_redirect_url(self, *args: Any, **kwargs: Any) -> str | None:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_list', kwargs={'slug': self.slug, 'order_by': '-created_at'})
    
    def post(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.object.update_upvotes(self.user)
        return super().post(request, *args, **kwargs)
    
    @cached_property
    def user(self):
        return self.request.user
    
    @cached_property
    def type(self):
        return self.kwargs.get('type')
    
    @cached_property
    def object(self):
        pk = self.kwargs.get('pk')
        match self.type:
            case 'thread':
                object = get_object_or_404(Thread, pk=pk)
            case 'reply':
                object = get_object_or_404(Reply, pk=pk)
            case _:
                raise Http404('Invalid content parameters!')
        return object
    
    @cached_property
    def slug(self):
        match self.type:
            case 'thread':
                slug = self.object.category.slug # type: ignore
            case 'reply':
                slug = self.object.thread.category.slug # type: ignore
            case _:
                raise Http404('Invalid content parameters!')
        return slug


class DeleteView(LoginRequiredMixin, UserPassesTestMixin, generic.RedirectView):
    permanent = False

    def get_redirect_url(self, *args: Any, **kwargs: Any) -> str | None:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_list', kwargs={'slug': self.slug, 'order_by': '-created_at'})
    
    def post(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.object.soft_delete()
        return super().post(request, *args, **kwargs)
    
    def test_func(self) -> bool | None:
        return self.request.user.is_staff or self.object.author == self.request.user
    
    @cached_property
    def object(self):
        pk = self.kwargs.get('pk')
        match self.type:
            case 'thread':
                object = get_object_or_404(Thread, pk=pk)
            case 'reply':
                object = get_object_or_404(Reply, pk=pk)
            case _:
                raise Http404('Invalid content parameters!')
        return object
    
    @cached_property
    def slug(self):
        match self.type:
            case 'thread':
                slug = self.object.category.slug # type: ignore
            case 'reply':
                slug = self.object.thread.category.slug # type: ignore
            case _:
                raise Http404('Invalid content parameters!')
        return slug

    @cached_property
    def type(self):
        return self.kwargs.get('type')


class LockView(LoginRequiredMixin, UserPassesTestMixin, generic.RedirectView):
    permanent = False

    def get_redirect_url(self, *args: Any, **kwargs: Any) -> str | None:
        next = self.request.GET.get('next')
        if next and url_has_allowed_host_and_scheme(url=next, allowed_hosts=settings.ALLOWED_HOSTS):
            return next
        return reverse_lazy('threads:thread_list', kwargs={'slug': self.slug, 'order_by': '-created_at'})

    def post(self, request: HttpRequest, *args: Any, **kwargs: Any) -> HttpResponse:
        self.object.update_lock()
        return super().post(request, *args, **kwargs)
    
    def test_func(self) -> bool | None:
        return self.request.user.is_staff
    
    @cached_property
    def object(self):
        pk = self.kwargs.get('pk')
        return get_object_or_404(Thread, pk=pk)
    
    @cached_property
    def slug(self):
        return self.object.category.slug
