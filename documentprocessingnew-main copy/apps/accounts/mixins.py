from django.contrib.auth.mixins import UserPassesTestMixin
from django.shortcuts import redirect
from django.conf import settings

class AdminRequiredMixin(UserPassesTestMixin):
    """
    Mixin to ensure the user is authenticated and has the ADMIN role.
    Redirects to the home page if the user is not an admin.
    """
    def test_func(self):
        from common.enums import UserRole
        return self.request.user.is_authenticated and (
            self.request.user.role == UserRole.ADMIN or self.request.user.is_superuser
        )

    def handle_no_permission(self):
        if not self.request.user.is_authenticated:
            return redirect(f"{settings.LOGIN_URL}?next={self.request.path}")
        else:
            # Safer fallback: redirect to a specific page instead of home (which might be the login page)
            if self.request.user.is_authenticated:
                return redirect('client_upload')
            return redirect('home')
