from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import UserCreationForm
from common.enums import UserRole

User = get_user_model()

class ClientRegistrationForm(UserCreationForm):
    email = forms.EmailField(required=True, help_text="Required. Inform a valid email address.")

    class Meta:
        model = User
        fields = ('username', 'email')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({'class': 'form-control'})


    def save(self, commit=True):
        user = super().save(commit=False)
        user.role = UserRole.CLIENT
        if commit:
            user.save()
        return user

class AdminCreationForm(UserCreationForm):
    email = forms.EmailField(required=True, help_text="Required. Inform a valid email address.")

    class Meta:
        model = User
        fields = ('username', 'email')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field in self.fields.values():
            field.widget.attrs.update({'class': 'form-control'})

    def save(self, commit=True):
        user = super().save(commit=False)
        user.role = UserRole.ADMIN
        user.is_staff = True # Admins are staff
        # user.is_superuser = False # Optional, depending on policy
        if commit:
            user.save()
        return user
