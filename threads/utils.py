from threads.models import *
from django.db.models import Count

def fuzzy_search(prompt: str):
    prompt = f'  {prompt.lower()}  '
    prompt_values = [prompt[i:i+3] for i in range(len(prompt) - 2)]
    return Thread.objects.filter(
        trigrams__value__in=prompt_values
    ).annotate(
        score=Count('trigrams')
    ).filter(
        score__gte=2
    ).order_by(
        '-score'
    )
