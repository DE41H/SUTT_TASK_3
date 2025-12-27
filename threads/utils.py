import threading
from django.core.mail import send_mail, send_mass_mail
from django.conf import settings
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

def queue_mail(to, subject: str, body: str):

    def send(to, subject, body):
        send_mail(
            subject=subject,
            message=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[to],
            fail_silently=True
        )

    threading.Thread(
        target=send,
        args=(to, subject, body)
    ).start()

def queue_mass_mail(messages):

    def send(messages):
        send_mass_mail(messages)

    threading.Thread(
        target=send,
        args=(messages, )
    ).start()
