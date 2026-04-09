from django.apps import AppConfig

class ProcessingConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.processing'

    def ready(self):
        import apps.processing.signals
