from django.urls import path
from .views import (
    RegisterDeviceView, HeartbeatView, NextBundleView, 
    DownloadBundleView, UploadResultView, PageUploadView,
    MergeDashboardView, MergeActionView, AcquireSpecificBundleView,
    BundleProcessingSyncView, BundleStatusView, BundleAdminActionView
)

urlpatterns = [
    # Device Management
    path("register/", RegisterDeviceView.as_view(), name="desktop-register"),
    path("<uuid:device_id>/heartbeat/", HeartbeatView.as_view(), name="desktop-heartbeat"),
    
    # Bundle Management
    path("<uuid:device_id>/next-bundle/", NextBundleView.as_view(), name="desktop-next-bundle"),
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/acquire/", AcquireSpecificBundleView.as_view(), name="desktop-acquire-bundle"),
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/download/", DownloadBundleView.as_view(), name="desktop-download-bundle"),
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/upload/", UploadResultView.as_view(), name="desktop-upload-bundle"),
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/upload-result/", UploadResultView.as_view(), name="desktop-upload-result"), # Hierarchical alias
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/upload-page/", PageUploadView.as_view(), name="desktop-upload-page"),
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/sync-processing/", BundleProcessingSyncView.as_view(), name="desktop-sync-processing"),
    path("<uuid:device_id>/bundles/<uuid:bundle_id>/status/", BundleStatusView.as_view(), name="desktop-bundle-status"),
    
    # Admin Interface
    path("admin/bundles/<uuid:bundle_id>/review/", BundleAdminActionView.as_view(), name="desktop-admin-bundle-review"),

    # Enterprise Reconstruction Control
    path("merge-dashboard/<uuid:document_id>/", MergeDashboardView.as_view(), name="desktop-merge-dashboard"),
    path("merge-action/<uuid:document_id>/", MergeActionView.as_view(), name="desktop-merge-action"),
]
