from django.contrib import admin
from django.urls import path, include
from django.views.generic import TemplateView, RedirectView
from django.contrib.auth import views as auth_views
from django.contrib.auth.decorators import login_required
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from apps.accounts.redirect_view import RoleBasedRedirectView
from apps.accounts.views import (
    RegisterView, AdminDashboardView, CreateResourceView, ResourceListView, 
    ClientListView, CreateAdminView, CreateClientView, AdminUploadView, 
    AdminDocumentListView, CustomLogoutView
)
from apps.processing.views import workspace_view

schema_view = get_schema_view(
    openapi.Info(
        title="DocPro API",
        default_version='v1',
        description="Document Processing Platform API",
    ),
    public=True,
    permission_classes=[permissions.AllowAny],
)

urlpatterns = [
    path('', auth_views.LoginView.as_view(template_name='auth/login.html', redirect_authenticated_user=True), name='home'),
    path('django-admin/', admin.site.urls),
    path('api/v1/auth/', include('apps.accounts.urls')),
    path('api/v1/documents/', include('apps.documents.urls')),
    path('api/v1/processing/', include('apps.processing.urls')),
    path('api/v1/audit/', include('apps.audit.urls')),
    path('api/v1/desktop/', include('apps.desktop_bridge.urls')),

    # Swagger
    path('swagger/', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),

    # Frontend Routes
    # Auth
    path('accounts/login/', auth_views.LoginView.as_view(template_name='auth/login.html', redirect_authenticated_user=True), name='login'),
    path('accounts/profile/', RoleBasedRedirectView.as_view(), name='role_redirect'),
    # path('admin/login/', ...), # Removed per request for single login page
    path('accounts/logout/', CustomLogoutView.as_view(next_page='/'), name='logout'),
    path('accounts/register/', RegisterView.as_view(), name='register'),
    
    # Client
    path('client/upload/', login_required(TemplateView.as_view(template_name='client/upload.html')), name='client_upload'),
    path('client/status/', login_required(TemplateView.as_view(template_name='client/status.html')), name='client_status'),
    path('client/document/<uuid:pk>/', login_required(TemplateView.as_view(template_name='client/document_detail.html')), name='client_document_detail'),
    
    # Resource
    path('resource/', RedirectView.as_view(pattern_name='resource_fetch', permanent=True)),
    path('resource/fetch/', login_required(TemplateView.as_view(template_name='resource/fetch_work.html')), name='resource_fetch'),
    path('workspace/<str:doc_ref>/<int:page_number>/', login_required(workspace_view), name='edit_assignment'),
    path('resource/history/', login_required(TemplateView.as_view(template_name='resource/history.html')), name='resource_history'),
    path('resource/profile/', login_required(TemplateView.as_view(template_name='resource/profile.html')), name='resource_profile'),
    path('resource/submit/', login_required(TemplateView.as_view(template_name='resource/submit_work.html')), name='resource_submit'),
    
    # Dashboard (Admin/General)
    path('dashboard/', RedirectView.as_view(pattern_name='admin_panel:dashboard', permanent=True)),
    path('admin/', include(([
        path('', RedirectView.as_view(pattern_name='admin_panel:dashboard', permanent=False), name='index'),
        path('dashboard/', login_required(AdminDashboardView.as_view()), name='dashboard'),
        path('resource/create/', login_required(CreateResourceView.as_view()), name='create_resource'),
        path('admins/create/', login_required(CreateAdminView.as_view()), name='create_admin'),
        path('clients/create/', login_required(CreateClientView.as_view()), name='create_client'),
        path('resources/', login_required(ResourceListView.as_view()), name='resource_list'),
        path('clients/', login_required(ClientListView.as_view()), name='client_list'),
        path('upload/', login_required(AdminUploadView.as_view()), name='upload_page'),
        path('documents/', login_required(AdminDocumentListView.as_view()), name='document_list'),
    ], 'admin_panel'), namespace='admin_panel')),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += staticfiles_urlpatterns()
