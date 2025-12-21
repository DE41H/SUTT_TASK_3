from typing import Any
from django.db.models.query import QuerySet
from django.shortcuts import get_object_or_404
from django.views import generic
from threads.models import *

# Create your views here.

class CategoryListView(generic.ListView):
    model = Category
    template_name = 'forum/category_list.html'
    context_object_name = 'categories'


class CategoryDetailView(generic.ListView):
    model = Thread
    template_name = 'forum/category_detail.html'
    context_object_name = 'threads'
    paginate_by = 10

    def get_queryset(self) -> QuerySet[Any]:
        self.category = get_object_or_404(Category, slug=self.kwargs['category_slug'])
        return Thread.objects.filter(category=self.category).order_by('-created_at')
    
    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        context['category'] = self.category
        return context
