from django.urls import path, include
from rest_framework.routers import DefaultRouter
from apps.processing import views

router = DefaultRouter()
router.register(r'workspace', views.WorkspaceViewSet, basename='workspace')
router.register(r'admin', views.ProcessingAdminViewSet, basename='admin')
router.register(r'tables', views.PageTableViewSet, basename='tables')
router.register(r'images', views.PageImageViewSet, basename='images')

urlpatterns = [
    # Layout Overhaul API
    path('pages/<int:page_id>/blocks/', views.PageBlocksAPIView.as_view(), name='page-blocks-api'),
    path('pages/<int:page_id>/analyze/', views.PageAnalyzeView.as_view(), name='page-analyze'),
    path('pages/<int:page_id>/recognize/', views.PageRecognizeView.as_view(), name='page-recognize'),
    path('pages/<int:page_id>/tables/save/', views.PageTableSaveView.as_view(), name='page-table-save'),
    path('blocks/<uuid:block_id>/save/', views.BlockSaveView.as_view(), name='block-save'),
    path('tables/<str:table_id>/cell/save/', views.TableCellSaveView.as_view(), name='table-cell-save'),

    # Explicit routes for the Workspace Workspace
    path('workspace/content/<str:doc_ref>/<int:page_number>/', views.WorkspaceViewSet.as_view({'get': 'get_workspace_data'}), name='workspace-data'),
    path('workspace/content/<str:doc_ref>/<int:page_number>/start/', views.WorkspaceViewSet.as_view({'post': 'start_processing'}), name='workspace-start'),
    path('workspace/content/<str:doc_ref>/<int:page_number>/submit/', views.WorkspaceViewSet.as_view({'post': 'submit_processing'}), name='workspace-submit'),
    
    # Capacity & Rebalancing
    path('resources/<int:resource_id>/capacity/', views.ResourceCapacityUpdateView.as_view(), name='resource-capacity-update'),
    
    # Real-time Auto-refresh API
    path('heartbeat/', views.heartbeat, name='heartbeat'),
    path('admin/dashboard-summary/', views.AdminDashboardSummaryView.as_view(), name='admin-dashboard-summary'),
    path('admin/resources/status/', views.ResourceStatusListView.as_view(), name='resource-status'),
    path('admin/documents/refresh/', views.DocumentListRefreshView.as_view(), name='document-list-refresh'),
    path('admin/submitted-queue/', views.SubmittedPagesQueueView.as_view(), name='submitted-queue'),
    path('queue/', views.AssignmentQueueView.as_view(), name='assignment-queue'),

    path('', include(router.urls)),
]
