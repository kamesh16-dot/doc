from django.shortcuts import redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views import View
from common.enums import UserRole

class RoleBasedRedirectView(LoginRequiredMixin, View):
    def get(self, request, *args, **kwargs):
        user = request.user
        if user.role == 'ADMIN' or user.is_superuser:
            return redirect('admin_panel:dashboard')
        elif user.role == 'CLIENT':
            return redirect('client_upload')
        elif user.role == 'RESOURCE':
            return redirect('resource_fetch')
        else:
            return redirect('client_upload')
