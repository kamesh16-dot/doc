def admin_context(request):
    """
    Context processor to add 'is_admin' variable to all templates.
    """
    if request.user.is_authenticated:
        return {'is_admin': request.user.role == 'ADMIN'}
    return {'is_admin': False}
