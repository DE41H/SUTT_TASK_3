import random
import threading
from django.core.mail import send_mail, send_mass_mail
from django.conf import settings

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
        send_mass_mail(messages, fail_silently=True)

    threading.Thread(
        target=send,
        args=(messages, )
    ).start()

def generate_random_color():
    return hex(random.randint(0x000000, 0xFFFFFF))
