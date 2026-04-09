import os
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.environ.get('SECRET_KEY', 'django-insecure-key-dev-12345')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['*','ec2-13-126-44-206.ap-south-1.compute.amazonaws.com']

# Security Settings for Local Development
SECURE_CROSS_ORIGIN_OPENER_POLICY = None
CSRF_TRUSTED_ORIGINS = [
    'http://localhost:8000', 
    'http://127.0.0.1:8000', 
    'http://0.0.0.0:8000',
    'http://*.ngrok-free.app', # Helpful for mobile/remote testing
]

# Increase upload limits for better performance with large PDFs
FILE_UPLOAD_MAX_MEMORY_SIZE = 104857600  # 100MB
DATA_UPLOAD_MAX_MEMORY_SIZE = 104857600  # 100MB
FILE_UPLOAD_PERMISSIONS = 0o644
CSRF_COOKIE_HTTPONLY = False  # Allow JS to read CSRF for debugging/AJAX
CSRF_COOKIE_SAMESITE = 'Lax'
CSRF_COOKIE_SECURE = False
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_NAME = 'csrftoken'




# Application definition

INSTALLED_APPS = [
    'daphne',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # Third party
    'rest_framework',
    'corsheaders',
    'drf_yasg',

    'channels',
    
    # Local apps
    'apps.accounts',
    'apps.documents',
    'apps.processing',
    'apps.audit',
    
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'apps.accounts.middleware.ActiveUserMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

X_FRAME_OPTIONS = 'SAMEORIGIN'

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR.parent / 'frontend' / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'apps.accounts.context_processors.admin_context',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'
ASGI_APPLICATION = 'config.asgi.application'

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels.layers.InMemoryChannelLayer',
    },
}


# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases

# import os
# import dj_database_url
# from dotenv import load_dotenv

# load_dotenv()

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.postgresql',
#         'NAME': os.environ.get('DB_NAME', 'docpro_db'),
#         'USER': os.environ.get('DB_USER', 'docpro_user'),
#         'PASSWORD': os.environ.get('DB_PASSWORD', 'newstrongpassword'),
#         'HOST': os.environ.get('DB_HOST', 'localhost'),
#         'PORT': os.environ.get('DB_PORT', '5433'),
#         'OPTIONS': {
#             'options': '-c search_path=docpro'
#         }
#     }
# }

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.postgresql',
#         'NAME': 'doc1',
#         'USER': 'postgres',
#         'PASSWORD': 'NewStrongPassword@123',
#         'HOST': 'localhost',
#         'PORT': '5433',
#     }
# }



# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.postgresql',
#         'NAME': 'documentprocessing',
#         'USER': 'xbrl_user',
#         'PASSWORD': 'StrongPassword@2026',
#         'HOST': 'localhost',   # IMPORTANT
#         'PORT': '5432',
#     }
# }

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'docprodb',
        'USER': 'xbrl_user',
        'PASSWORD': 'StrongPassword@2026',
        'HOST': 'localhost',
        'PORT': '5433',
    }
}


CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'unique-snowflake',
    }
}


# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    # {
    #     'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    # },
    # {
    #     'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    # },
    # {
    #     'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    # },
    # {
    #     'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    # },
]


# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'

STATICFILES_DIRS = [
    BASE_DIR.parent / 'frontend' / 'static',
]

MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR.parent / 'media'

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Custom User Model
# Custom User Model
AUTH_USER_MODEL = 'accounts.User'

# Auth Redirects
LOGIN_REDIRECT_URL = 'role_redirect'
LOGOUT_REDIRECT_URL = 'home'
LOGIN_URL = 'login'

# Celery Configuration
# CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://redis:6379/0')
# CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')
CELERY_BROKER_URL = 'memory://'
CELERY_RESULT_BACKEND = "db+postgresql://xbrl_user:StrongPassword%402026@localhost:5433/docprodb"
CELERY_TASK_ALWAYS_EAGER = True
CELERY_TASK_EAGER_PROPAGATES = True
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'UTC'

CELERY_BEAT_SCHEDULE = {
    'mark-inactive-resources': {
        'task':     'tasks.mark_inactive_resources',
        'schedule': 60.0,   # every 60 seconds
    },
}

# REST Framework
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        # 'rest_framework.authentication.BasicAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ),
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 10
}

# OCR Configuration
OCR_APP_ID = os.environ.get('OCR_APP_ID', 'test_app_id')
OCR_PASSWORD = os.environ.get('OCR_PASSWORD', 'test_password')
# Online Status Configuration
USER_ONLINE_TIMEOUT_MINUTES = 3

OCR_BASE_URL = os.environ.get('OCR_BASE_URL', 'https://cloud-westul.ocrsdk.com/v2/')
# Azure Form Recognizer (DocPro: Better free tier + Tables)
AZURE_OCR_ENDPOINT = os.environ.get('AZURE_OCR_ENDPOINT', 'https://{your-endpoint}.cognitiveservices.azure.com/')
AZURE_OCR_KEY = os.environ.get('AZURE_OCR_KEY', 'XXXX-XXXX-XXXX-XXXX-XXXX')
# Adobe PDF Services (DocPro: High-fidelity extraction + Generation)
ADOBE_CLIENT_ID = os.environ.get('ADOBE_CLIENT_ID', '8f88a1579d614b29bf3474b86c22ffc9')
ADOBE_CLIENT_SECRET = os.environ.get('ADOBE_CLIENT_SECRET', 'XXXX-XXXX-XXXX-XXXX-XXXX')



# Logging Configuration to silence heartbeat noise
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'skip_heartbeat': {
            '()': 'django.utils.log.CallbackFilter',
            'callback': lambda record: '/heartbeat/' not in record.getMessage(),
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'filters': ['skip_heartbeat'],
        },
    },
    'loggers': {
        '': { # Root logger
            'handlers': ['console'],
            'level': 'INFO',
        },
        'django.server': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'apps': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}
