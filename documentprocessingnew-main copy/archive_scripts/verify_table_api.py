
import os
import django
import uuid

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from apps.documents.models import Page, Block, PageTable
from apps.processing.views import WorkspaceViewSet, PageTableSaveView
from rest_framework.test import APIRequestFactory, force_authenticate
from django.contrib.auth import get_user_model

User = get_user_model()

def verify_table_ops():
    # 1. Setup
    factory = APIRequestFactory()
    user = User.objects.filter(is_superuser=True).first()
    if not user:
        user = User.objects.create_superuser('admin_test', 'admin@test.com', 'pass')
    
    page = Page.objects.first()
    if not page:
        print("No pages found in database. Cannot run verification.")
        return

    print(f"Testing with Page ID: {page.id}")

    # 2. Test Create Table (3x3 default)
    viewset = WorkspaceViewSet.as_view({'post': 'create_block'})
    data = {
        'type': 'table',
        'x': 100, 'y': 100, 'width': 300, 'height': 200,
        'row_count': 3, 'col_count': 3
    }
    request = factory.post(f'/api/v1/processing/pages/{page.id}/blocks/create/', data, format='json')
    force_authenticate(request, user=user)
    response = viewset(request, page_id=page.id)
    
    assert response.status_code == 200
    table_ref = response.data['table_ref']
    print(f"Created table with ref: {table_ref}")

    table_obj = PageTable.objects.get(table_ref=table_ref)
    assert table_obj.row_count == 3
    assert table_obj.col_count == 3
    assert len(table_obj.table_json) == 3
    assert len(table_obj.table_json[0]) == 3
    print("SUCCESS: 3x3 Table creation verified.")

    # 3. Test Structural Update (Add Row -> 4x3)
    save_view = PageTableSaveView.as_view()
    updated_json = table_obj.table_json + [[{'text': 'New Row', 'indent': 0}] * 3]
    update_data = {
        'table_ref': table_ref,
        'row_count': 4,
        'col_count': 3,
        'table_json': updated_json,
        'x': 100, 'y': 100, 'width': 300, 'height': 200
    }
    request = factory.post(f'/api/v1/processing/pages/{page.id}/tables/save/', update_data, format='json')
    force_authenticate(request, user=user)
    response = save_view(request, page_id=page.id)
    
    assert response.status_code == 200
    table_obj.refresh_from_db()
    assert table_obj.row_count == 4
    assert len(table_obj.table_json) == 4
    print("SUCCESS: Row addition (structural save) verified.")

    print("\nALL BACKEND VERIFICATIONS PASSED!")

if __name__ == "__main__":
    verify_table_ops()
