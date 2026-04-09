from datetime import timedelta
from django.utils import timezone
from rest_framework import viewsets, permissions, status, views
from rest_framework.response import Response
from rest_framework.decorators import action
from django.contrib.auth import get_user_model
from apps.accounts.serializers import UserSerializer, RegisterSerializer, ResourceProfileSerializer, PasswordChangeSerializer
from apps.accounts.models import ResourceProfile
from common.enums import UserRole
import logging
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework.authentication import SessionAuthentication, TokenAuthentication

class CsrfExemptSessionAuthentication(SessionAuthentication):
    def enforce_csrf(self, request):
        return  # Skip CSRF check for this specific authentication class

logger = logging.getLogger(__name__)

User = get_user_model()

# Frontend Views
from django.views.generic import CreateView, ListView, TemplateView
from django.urls import reverse_lazy
from django.contrib.auth import logout
from django.contrib.auth.views import LogoutView
from django.shortcuts import redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from apps.accounts.mixins import AdminRequiredMixin
from django.db.models import Q
from apps.accounts.forms import ClientRegistrationForm, AdminCreationForm

class RegisterView(CreateView):
    template_name = 'auth/register.html'
    form_class = ClientRegistrationForm
    success_url = reverse_lazy('login')

    def form_valid(self, form):
        # Optional: Add success message
        return super().form_valid(form)

    def form_invalid(self, form):
        logger.warning(f"Register Form Invalid: {form.errors}")
        return super().form_invalid(form)

from django.contrib.messages.views import SuccessMessageMixin

class CreateAdminView(AdminRequiredMixin, SuccessMessageMixin, CreateView):
    template_name = 'admin/create_admin.html'
    form_class = AdminCreationForm
    success_url = reverse_lazy('admin_panel:dashboard')
    success_message = "Admin account created successfully!"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Add any extra context if needed
        return context

class CreateClientView(AdminRequiredMixin, CreateView):
    template_name = 'admin/create_client.html'
    form_class = ClientRegistrationForm
    success_url = reverse_lazy('admin_panel:client_list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

class UserBaseListView(AdminRequiredMixin, ListView):
    model = User
    context_object_name = 'users'
    paginate_by = 10

    def get_queryset(self):
        from django.db.models import Exists, OuterRef
        from apps.processing.models import Assignment
        from common.enums import AssignmentStatus
        
        queryset = User.objects.select_related('resource_profile').all()
        
        # Annotate if the user currently has an assigned document
        from apps.processing.models import PageAssignment
        from common.enums import PageAssignmentStatus
        active_assignments = PageAssignment.objects.filter(
            resource__user=OuterRef('pk'),
            status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
        )
        queryset = queryset.annotate(_is_working=Exists(active_assignments))
        
        # Status Filtering
        status = self.request.GET.get('status')
        if status == 'ACTIVE':
            queryset = queryset.filter(is_active=True)
        elif status == 'INACTIVE':
            queryset = queryset.filter(is_active=False)
        elif status == 'ONLINE':
            from django.utils import timezone
            from datetime import timedelta
            # Online if heartbeat within 30 seconds
            threshold = timezone.now() - timedelta(seconds=30)
            queryset = queryset.filter(resource_profile__last_seen__gte=threshold)
        elif status == 'OFFLINE':
            from django.utils import timezone
            from datetime import timedelta
            # Offline if no heartbeat for > 2 minutes or never seen
            threshold = timezone.now() - timedelta(seconds=120)
            queryset = queryset.filter(Q(resource_profile__last_seen__lt=threshold) | Q(resource_profile__last_seen__isnull=True))
        elif status == 'FULL':
            # At capacity (status is BUSY)
            queryset = queryset.filter(resource_profile__status='BUSY')
        elif status == 'FREE':
            # Available/Free (status is ACTIVE and has remaining capacity)
            queryset = queryset.filter(resource_profile__status='ACTIVE')

        # Search
        search_query = self.request.GET.get('search')
        if search_query:
            queryset = queryset.filter(
                Q(username__icontains=search_query) |
                Q(email__icontains=search_query)
            )

        # Sorting
        sort = self.request.GET.get('sort', 'newest')
        if sort == 'newest':
            queryset = queryset.order_by('-id')
        elif sort == 'oldest':
            queryset = queryset.order_by('id')
        elif sort == 'az':
            queryset = queryset.order_by('username')
        elif sort == 'za':
            queryset = queryset.order_by('-username')
        else:
             queryset = queryset.order_by('-id') # Default check

        return queryset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        status = self.request.GET.get('status', 'ALL')
        context['current_status'] = status
        context['is_status_all'] = status == 'ALL'
        context['is_status_active'] = status == 'ACTIVE'
        context['is_status_inactive'] = status == 'INACTIVE'
        context['current_search'] = self.request.GET.get('search', '')
        context['current_sort'] = self.request.GET.get('sort', 'newest')
        return context

from apps.documents.models import Document
from common.enums import DocumentStatus

class AdminDashboardView(UserBaseListView):
    template_name = 'admin/dashboard.html'
    
    def get_queryset(self):
        return super().get_queryset().filter(Q(role=UserRole.ADMIN) | Q(is_superuser=True))

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Dashboard Stats
        context['total_users'] = User.objects.count()
        context['total_resources'] = User.objects.filter(role=UserRole.RESOURCE).count()
        context['active_resources'] = User.objects.filter(role=UserRole.RESOURCE, resource_profile__status='ACTIVE').count()
        context['total_clients'] = User.objects.filter(role=UserRole.CLIENT).count()
        context['total_admins'] = User.objects.filter(role=UserRole.ADMIN).count()
        
        context['total_docs'] = Document.objects.count()
        context['processing_docs'] = Document.objects.filter(status__in=[DocumentStatus.ASSIGNED, DocumentStatus.IN_PROGRESS]).count()
        context['pending_reviews'] = Document.objects.filter(status=DocumentStatus.REVIEWING).count()
        
        # Completed Today
        today = timezone.now().date()
        context['completed_docs'] = Document.objects.filter(
            status=DocumentStatus.COMPLETED,
            completed_at__date=today
        ).count()

        # Detailed Page Stats for Assignment Rules
        from apps.documents.models import Page
        from common.enums import PageStatus
        context['assigned_pages_count'] = Page.objects.filter(status=PageStatus.ASSIGNED).count()
        context['unassigned_pages_count'] = Page.objects.filter(status__in=[PageStatus.PENDING, PageStatus.UNASSIGNED]).count()
        context['unassigned_docs_count'] = Document.objects.filter(status__in=[DocumentStatus.UPLOADED, DocumentStatus.SPLITTING, DocumentStatus.READY]).count()
        
        # Consistent Resource Mapping
        context['total_resources'] = context['total_resources'] # Already counts role=RESOURCE
        context['active_resources'] = context['active_resources'] # Already counts status=ACTIVE
        
        return context

class ResourceListView(UserBaseListView):
    template_name = 'admin/resource_list.html'
    
    def get_queryset(self):
        return super().get_queryset().filter(role=UserRole.RESOURCE)

class ClientListView(UserBaseListView):
    template_name = 'admin/client_list.html'
    
    def get_queryset(self):
        return super().get_queryset().filter(role=UserRole.CLIENT)

class CreateResourceView(AdminRequiredMixin, TemplateView):
    template_name = 'admin/create_resource.html'

class AdminUploadView(AdminRequiredMixin, TemplateView):
    template_name = 'admin/upload.html'

class AdminDocumentListView(AdminRequiredMixin, TemplateView):
    template_name = 'admin/documents.html'

class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer

    def get_permissions(self):
        if self.action == 'create':
            return [permissions.AllowAny()]
        return [permissions.IsAuthenticated()]

    def get_serializer_class(self):
        if self.action == 'create':
            return RegisterSerializer
        return UserSerializer

    def perform_update(self, serializer):
        user = serializer.instance
        
        # Security: Prevent deactivation of superusers
        if user.is_superuser and not serializer.validated_data.get('is_active', user.is_active):
            from rest_framework.exceptions import ValidationError
            raise ValidationError({"error": "Superusers cannot be disabled."})
        
        # Handle password change if provided in data
        password = self.request.data.get('password')
        if password and len(password.strip()) >= 8:
            user.set_password(password.strip())
            # Important: we don't save(user) here yet to avoid a double-save if possible,
            # but serializer.save() will call instance.save().
            # Note: set_password on instance will be reflected in serializer.save().

        # Handle nested resource profile update
        resource_profile_data = self.request.data.get('resource_profile')
        if resource_profile_data and hasattr(user, 'resource_profile'):
            profile = user.resource_profile
            # Use data.get if nested, else stick to current
            new_capacity = resource_profile_data.get('max_capacity')
            if new_capacity is not None:
                profile.max_capacity = new_capacity
                profile.save()

        # Finalize the main serializer save (this persists all User fields + hashed password)
        serializer.save()

    def destroy(self, request, *args, **kwargs):
        user = self.get_object()
        if user.is_superuser:
            return Response(
                {"error": "Superusers cannot be deleted."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            return super().destroy(request, *args, **kwargs)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error deleting user {user.id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            return Response(
                {"error": f"Deletion failed: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['post'], permission_classes=[permissions.IsAuthenticated])
    def change_password(self, request):
        serializer = PasswordChangeSerializer(data=request.data)
        if serializer.is_valid():
            user = request.user
            if not user.check_password(serializer.validated_data['old_password']):
                return Response({"old_password": ["Wrong password."]}, status=status.HTTP_400_BAD_REQUEST)
            
            user.set_password(serializer.validated_data['new_password'])
            user.save()
            return Response({"status": "password set"})
    @method_decorator(csrf_exempt)
    @action(detail=False, methods=['post'], permission_classes=[permissions.IsAuthenticated],
            authentication_classes=[CsrfExemptSessionAuthentication, TokenAuthentication])
    def heartbeat(self, request):
        """
        Endpoint to explicitly track active status.
        Updates last_active_at and marks ResourceProfile as ACTIVE.
        """
        user = request.user
        now = timezone.now()
        
        try:
            # Keep user model up to date
            import datetime
            
            # Throttle updates to avoid DB thrashing. Use .update() to bypass signals.
            if not user.last_activity or (now - user.last_activity) > datetime.timedelta(minutes=1):
                User.objects.filter(pk=user.pk).update(last_activity=now)
            
            if hasattr(user, 'resource_profile'):
                profile = user.resource_profile
                if profile.status == 'INACTIVE' or not profile.last_active_at or (now - profile.last_active_at) > datetime.timedelta(minutes=1):
                    # Instead of hardcoding 'ACTIVE', we set is_available and then refresh_status
                    # which will correctly set 'ACTIVE' or 'BUSY' based on load.
                    profile.is_available = True
                    profile.last_active_at = now
                    profile.last_seen = now # Sync with cleanup task
                    if profile.status == 'INACTIVE':
                        profile.status = 'ACTIVE' # Wake up
                    profile.save(update_fields=['is_available', 'last_active_at', 'last_seen', 'status'])
                    profile.refresh_status()
                    
                # Keep Redis cache fresh (TTL 90 seconds)
                from django.core.cache import cache
                cache.set(f"user:{user.id}:online", "true", 90)
                
                # Proactive Assignment: Only kick if document(s) are waiting
                from apps.processing.models import DocumentQueue
                from common.enums import QueueStatus
                if DocumentQueue.objects.filter(status=QueueStatus.WAITING).exists():
                    from apps.processing.tasks import assign_pages_task
                    assign_pages_task.delay()
                
            return Response({'status': 'active', 'timestamp': now})
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Heartbeat failed for user {user.id}: {str(e)}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class CustomLoginView(views.APIView):
    """Placeholder or override if login API is used, though standard Django auth might be used."""
    # This is handled normally by DRF tokens or session login, but we need
    # to hook into the login signal to update last_login_at.
    pass

class CustomLogoutView(LogoutView):
    def post(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            user = request.user
            user.last_activity = None
            user.save(update_fields=['last_activity'])
            
            if hasattr(user, 'resource_profile'):
                profile = user.resource_profile
                profile.status = 'INACTIVE'
                profile.is_available = False
                # Force online_status to 'offline' by setting last_seen way back
                profile.last_seen = timezone.now() - timedelta(days=1)
                profile.save(update_fields=['status', 'is_available', 'last_seen'])
                
            from django.core.cache import cache
            cache.delete(f"user:{user.id}:online")
            
        return super().post(request, *args, **kwargs)

class ResourceViewSet(viewsets.ReadOnlyModelViewSet):
    """
    Admin ViewSet to manage Resources.
    """
    queryset = ResourceProfile.objects.all()
    serializer_class = ResourceProfileSerializer
    permission_classes = [permissions.IsAdminUser]

    @action(detail=True, methods=['post'])
    def toggle_active(self, request, pk=None):
        resource = self.get_object()
        resource.is_active_for_assignment = not resource.is_active_for_assignment
        resource.save()
        return Response({'status': 'updated', 'is_active_for_assignment': resource.is_active_for_assignment})

# Gracefully handle CSRF mismatches (often caused by multiple tabs or expired sessions)
def csrf_failure(request, reason=""):
    logger.warning(f"CSRF Failure: {reason} for path {request.path}")
    from django.shortcuts import redirect
    from django.contrib import messages
    from django.http import JsonResponse

    # Detect API/AJAX requests and return 403 instead of a redirect
    if request.path.startswith('/api/') or request.headers.get('x-requested-with') == 'XMLHttpRequest':
        return JsonResponse({'detail': f'CSRF token incorrect or missing: {reason}'}, status=403)

    # Prevent infinite redirects if the login page itself is failing
    if request.path == '/accounts/login/':
        messages.error(request, "Session expired or invalid form submission. Please clear cookies or try again.")
        return redirect('login')
        
    messages.error(request, "Session expired or invalid form submission. Please try again.")
    referer = request.META.get('HTTP_REFERER')
    if referer:
        return redirect(referer)
    return redirect('login')
