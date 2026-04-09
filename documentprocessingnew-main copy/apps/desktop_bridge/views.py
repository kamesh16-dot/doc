from __future__ import annotations

import hashlib
import io
import json
import zipfile
import logging
from pathlib import Path
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db import transaction
from django.http import FileResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import permissions, status
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view, permission_classes, authentication_classes
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import DesktopDevice, AssignmentBundle, MergeManifest, PageVersion, UploadedPDF
from .serializers import DesktopDeviceSerializer, AssignmentBundleSerializer
from .services import (
    make_bundle_zip, sha256_fileobj, register_upload, 
    PageVersionResolver, ZeroLossMergeEngine,
    trigger_merge_if_ready, reassign_bundle
)
from .tasks import task_deterministic_merge

logger = logging.getLogger(__name__)

# Configurable constants
LEASE_MINUTES = 60


class RegisterDeviceView(APIView):
    """
    Initial pairing of a Desktop Agent.
    Returns the persistent Token needed for subsequent API calls.
    """
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        device_name   = request.data.get("device_name", "").strip()
        machine_id    = request.data.get("machine_id", "").strip()
        agent_version = request.data.get("agent_version", "1.0.0").strip()

        if not device_name or not machine_id:
            return Response(
                {"detail": "device_name and machine_id are required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )

        # Update or create the device entry
        device, created = DesktopDevice.objects.update_or_create(
            user=request.user,
            machine_id=machine_id,
            defaults={
                "device_name": device_name,
                "agent_version": agent_version,
                "is_active": True,
                "last_heartbeat_at": timezone.now(),
            }
        )
        
        token, _ = Token.objects.get_or_create(user=request.user)

        return Response(
            {
                "device": DesktopDeviceSerializer(device).data,
                "api_token": token.key,
                "lease_minutes": LEASE_MINUTES,
                "created": created
            }
        )


class HeartbeatView(APIView):
    """
    Called by the agent every N seconds to maintain 'online' status.
    Used for revoking leases if the device disappears.
    """
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, device_id):
        try:
            device = DesktopDevice.objects.get(id=device_id, user=request.user)
        except (DesktopDevice.DoesNotExist, ValueError):
            return Response({"detail": "Device not found or unauthorized"}, status=status.HTTP_404_NOT_FOUND)

        device.last_heartbeat_at = timezone.now()
        device.last_seen_ip     = request.META.get("REMOTE_ADDR")
        device.agent_version    = request.data.get("agent_version", device.agent_version)
        device.save(update_fields=["last_heartbeat_at", "last_seen_ip", "agent_version", "updated_at"])
        return Response({"status": "healthy", "server_time": timezone.now().isoformat()})


class NextBundleView(APIView):
    """
    Polling endpoint for the agent to request the next available assignment.
    Implements a lease-lock to prevent multiple agents grabbing the same bundle.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, device_id):
        try:
            device = DesktopDevice.objects.get(id=device_id, user=request.user, is_active=True)
        except (DesktopDevice.DoesNotExist, ValueError):
            return Response({"detail": "Active device not found or unauthorized"}, status=status.HTTP_404_NOT_FOUND)

        # Atomic lease acquisition
        # We find bundles where status is READY or EXPIRED.
        try:
            with transaction.atomic():
                from apps.processing.models import PageAssignment, PageAssignmentStatus
                
                # Filter for documents where the requesting user has active assignments
                assigned_doc_ids = PageAssignment.objects.filter(
                    resource__user=request.user,
                    status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                ).values_list('page__document_id', flat=True).distinct()

                bundle = (
                    AssignmentBundle.objects.select_for_update(skip_locked=True)
                    .filter(
                        status__in=[AssignmentBundle.Status.READY, AssignmentBundle.Status.EXPIRED],
                        document_id__in=assigned_doc_ids,
                        document__is_deleted=False
                    )
                    .order_by("document__priority", "document_id", "-bundle_index")
                    .select_related("document")
                    .first()
                )

                if not bundle:
                    return Response({"bundle": None}, status=status.HTTP_200_OK)

                # ── ENFORCE STRICT FLOW ──────────────────────────────────────────
                # Ensure the document and bundle have a Job ID
                if not bundle.document.job:
                    from apps.processing.models import Job
                    job = Job.objects.create(
                        name=f"Auto-Job: {bundle.document.name or 'Doc ' + str(bundle.document.id)[:8]}"
                    )
                    bundle.document.job = job
                    bundle.document.save(update_fields=["job"])
                    logger.info(f"Created Auto-Job {job.id} for Document {bundle.document.id}")

                if not bundle.job:
                    bundle.job = bundle.document.job
                    # No need to save yet, bundle.save() below handles it.

                bundle.status = AssignmentBundle.Status.LEASED
                bundle.leased_to = device
                bundle.lease_expires_at = timezone.now() + timedelta(minutes=LEASE_MINUTES)
                
                # Prepare the manifest for the agent
                bundle.manifest = {
                    "lease_token": str(bundle.lease_token),
                    "lease_expires_at": bundle.lease_expires_at.isoformat(),
                    "device": device.device_name
                }
                bundle.save() # Atomic update

                logger.info(f"Leased bundle {bundle.id} to device {device.id} (Job: {bundle.job_id})")
                return Response({"bundle": AssignmentBundleSerializer(bundle).data})
        except Exception as e:
            logger.exception(f"Error acquired bundle for device {device_id}: {e}")
            return Response({"detail": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class AcquireSpecificBundleView(APIView):
    """
    Explicitly leases a specific bundle to a device.
    Useful for URI commands like docpro://open/<bundle_id>.
    """
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, device_id, bundle_id):
        device = get_object_or_404(DesktopDevice, id=device_id, user=request.user, is_active=True)

        try:
            with transaction.atomic():
                from apps.processing.models import PageAssignment, PageAssignmentStatus

                # Verify user assignment (Admins bypass this)
                from common.enums import UserRole
                is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser

                bundle = get_object_or_404(AssignmentBundle, id=bundle_id)

                if not is_admin:
                    # Accept ANY assignment status — user may have completed the work
                    # and their assignments moved from ASSIGNED -> SUBMITTED
                    has_assignment = PageAssignment.objects.filter(
                        resource__user=request.user,
                        page__document=bundle.document,
                    ).exists()

                    if not has_assignment:
                        return Response({"detail": "User has no assignments for this document."}, status=status.HTTP_403_FORBIDDEN)

                bundle = AssignmentBundle.objects.select_for_update().get(id=bundle_id)

                # Allow claiming if: READY, EXPIRED, LEASED (re-claim), DOWNLOADED, or already this device's
                terminal_statuses = {AssignmentBundle.Status.COMPLETED, AssignmentBundle.Status.MERGED}
                # Allow claiming if: READY, EXPIRED, LEASED (re-claim), DOWNLOADED, or already this device's
                terminal_statuses = {AssignmentBundle.Status.COMPLETED, AssignmentBundle.Status.MERGED}
                if bundle.status in terminal_statuses and bundle.leased_to != device:
                    return Response({"detail": "Bundle is finalized and cannot be re-claimed."}, status=status.HTTP_409_CONFLICT)

                # ── ENFORCE STRICT FLOW ──────────────────────────────────────────
                if not bundle.document.job:
                    from apps.processing.models import Job
                    job = Job.objects.create(
                        name=f"Auto-Job: {bundle.document.name or 'Doc ' + str(bundle.document.id)[:8]}"
                    )
                    bundle.document.job = job
                    bundle.document.save(update_fields=["job"])
                    logger.info(f"Created Auto-Job {job.id} for Document {bundle.document.id}")

                if not bundle.job:
                    bundle.job = bundle.document.job

                # Acquire/Renew Lease
                bundle.status = AssignmentBundle.Status.LEASED
                bundle.leased_to = device
                bundle.lease_expires_at = timezone.now() + timedelta(minutes=LEASE_MINUTES)
                bundle.save()

                logger.info(f"Explicitly leased bundle {bundle_id} to device {device_id} (Job: {bundle.job_id})")
                return Response({
                    "ok": True,
                    "bundle": AssignmentBundleSerializer(bundle).data
                })
        except Exception as e:
            logger.exception(f"Error acquiring specific bundle {bundle_id}: {e}")
            return Response({"detail": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DownloadBundleView(APIView):
    """
    Streams the ZIP bundle to the agent.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, device_id, bundle_id):
        device = get_object_or_404(DesktopDevice, id=device_id, user=request.user, is_active=True)
        bundle = get_object_or_404(AssignmentBundle, id=bundle_id, leased_to=device)

        if bundle.lease_expires_at and bundle.lease_expires_at < timezone.now():
            bundle.status = AssignmentBundle.Status.EXPIRED
            bundle.save(update_fields=["status", "updated_at"])
            return Response({"detail": "Lease expired during download request"}, status=status.HTTP_409_CONFLICT)

        bundle.downloaded_at = timezone.now()
        bundle.status = AssignmentBundle.Status.DOWNLOADED
        bundle.save(update_fields=["downloaded_at", "status", "updated_at"])

        # SELF-HEALING: Check if source PDF is missing from disk
        if not bundle.source_pdf or not bundle.source_pdf.storage.exists(bundle.source_pdf.name):
            logger.warning(f"Bundle {bundle_id} source file is missing from storage. Attempting on-the-fly regeneration.")
            from .services import regenerate_bundle_file
            if not regenerate_bundle_file(bundle):
                return Response(
                    {"detail": "Bundle source file is missing and regeneration failed (master document might be missing)."},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        zip_bytes = make_bundle_zip(bundle)
        resp = HttpResponse(zip_bytes, content_type="application/zip")
        resp["Content-Disposition"] = f'attachment; filename="docpro_bundle_{bundle_id}.zip"'
        return resp


class UploadResultView(APIView):
    """
    Receives the edited PDF from the agent.
    Verifies SHA256 and triggers the document merge logic if all bundles are in.
    """
    permission_classes = [permissions.IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, device_id, bundle_id):
        device = get_object_or_404(DesktopDevice, id=device_id, user=request.user, is_active=True)
        
        # Step 1: Lock bundle and validate mapping
        try:
            with transaction.atomic():
                bundle = AssignmentBundle.objects.select_for_update().get(id=bundle_id)
                
                # Metadata from request
                req_job_id = request.data.get("job_id")
                req_doc_id = request.data.get("document_id")
                req_user_id = request.data.get("user_id") or str(request.user.id) # Fallback to current user if not sent
                
                # Step 2: Idempotency
                if bundle.status == AssignmentBundle.Status.COMPLETED:
                    return Response({"status": "already_completed", "ok": True}, status=status.HTTP_200_OK)

                # Step 3: Hierarchical Validation (ONLY if Job-based system is active for this doc)
                is_strict_flow = bundle.document.job_id is not None
                
                if is_strict_flow:
                    # Normalize for comparison
                    bid_match = str(bundle.id) == str(bundle_id)
                    jid_match = str(bundle.job_id) == str(req_job_id)
                    did_match = str(bundle.document_id) == str(req_doc_id)
                    
                    # user_id match: Allow if bundle.user is None OR matches req_user_id
                    uid_match = (bundle.user_id is None) or (str(bundle.user_id) == str(req_user_id))
                    
                    if not (bid_match and jid_match and did_match and uid_match):
                        logger.warning(
                            f"INVALID_MAPPING for upload: Dev={device_id} User={request.user.id} "
                            f"Req(Job={req_job_id}, Doc={req_doc_id}, User={req_user_id}) "
                            f"Actual(Job={bundle.job_id}, Doc={bundle.document_id}, User={bundle.user_id})"
                        )
                        return Response({
                            "detail": "INVALID_MAPPING: Hierarchical ID mismatch",
                            "meta": {
                                "expected_job": str(bundle.job_id),
                                "expected_doc": str(bundle.document_id),
                                "expected_user": str(bundle.user_id) if bundle.user_id else "ANY"
                            }
                        }, status=status.HTTP_403_FORBIDDEN)

                    if bundle.status != AssignmentBundle.Status.DOWNLOADED:
                        return Response({"detail": f"INVALID_STATE: Bundle status is {bundle.status}, expected DOWNLOADED"}, status=status.HTTP_400_BAD_REQUEST)

                # Step 4: Validate format
                uploaded_file = request.FILES.get("result_pdf") 
                if not uploaded_file:
                    return Response({"detail": "No file uploaded"}, status=status.HTTP_400_BAD_REQUEST)

                if is_strict_flow:
                    valid_exts = {".pdf", ".docx"}
                    # Path is now imported at the top of the file
                    ext = Path(uploaded_file.name).suffix.lower()
                    if ext not in valid_exts:
                        return Response({"detail": f"INVALID_FORMAT: Strict flow allows {valid_exts}, got {ext}"}, status=status.HTTP_400_BAD_REQUEST)

                # Integrity check (SHA256)
                expected_sha = request.data.get("sha256", "").strip().lower()
                actual_sha   = sha256_fileobj(uploaded_file.file)
                
                if expected_sha and expected_sha != actual_sha:
                    return Response(
                        {"detail": "Checksum mismatch. Upload corrupted.", "server_sha": actual_sha}, 
                        status=status.HTTP_409_CONFLICT
                    )

                # Step 5: Save file & Mark Progress
                if is_strict_flow:
                    # In Strict Flow, we use output_file field regardless of PDF/DOCX
                    bundle.output_file.save(uploaded_file.name, uploaded_file, save=False)
                    bundle.status = AssignmentBundle.Status.SUBMITTED
                    bundle.completed_at = timezone.now()
                else:
                    # Legacy Flow
                    bundle.result_pdf.save(uploaded_file.name, uploaded_file, save=False)
                    bundle.status = AssignmentBundle.Status.UPLOADED
                
                bundle.result_sha256 = actual_sha
                bundle.uploaded_at = timezone.now()
                bundle.save()

                # ── NEW: Synchronize Page Assignments ────────────────
                try:
                    from apps.processing.models import PageAssignment, SubmittedPage
                    from common.enums import PageAssignmentStatus, ReviewStatus
                    
                    # Find all active assignments for this document and these pages
                    assignments = PageAssignment.objects.filter(
                        document=bundle.document,
                        page__page_number__in=bundle.page_numbers,
                        status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                    )
                    
                    for assign in assignments:
                        assign.status = PageAssignmentStatus.SUBMITTED
                        assign.submitted_at = timezone.now()
                        assign.processing_end_at = timezone.now()
                        assign.save()
                        
                        # Create/Update SubmittedPage entry for the legacy review UI
                        SubmittedPage.objects.update_or_create(
                            assignment=assign,
                            defaults={
                                "page": assign.page,
                                "document": bundle.document,
                                "submitted_by": request.user,
                                "bundle": bundle,
                                "page_number": assign.page.page_number,
                                "output_page_file": bundle.output_file if is_strict_flow else bundle.result_pdf,
                                "review_status": ReviewStatus.PENDING_REVIEW,
                                "submitted_at": timezone.now()
                            }
                        )
                    logger.info(f"Synchronized {assignments.count()} page assignments for bundle {bundle_id}")
                except Exception as sync_err:
                    logger.error(f"Failed to synchronize page assignments for bundle {bundle_id}: {sync_err}")

                if not is_strict_flow:
                    # Legacy Enterprise Logic: Decompose into PageVersions
                    upload_record = register_upload(bundle, uploaded_file)
                    logger.info(f"Legacy Bundle {bundle_id} registered in ledger. Upload ID: {upload_record.id}")
                    # Trigger legacy deterministic merge
                    task_deterministic_merge.delay(str(bundle.document_id))
                else:
                    # New Strict Flow: Waiting for Admin Approval.
                    logger.info(f"Bundle {bundle_id} SUBMITTED for review.")

            return Response({
                "ok": True, 
                "bundle_id": str(bundle.id),
                "status": bundle.status,
                "sha256": actual_sha,
                "flow": "strict" if is_strict_flow else "legacy"
            })

        except AssignmentBundle.DoesNotExist:
            return Response({"detail": "Bundle not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            logger.exception(f"Failed to process upload for bundle {bundle_id}")
            return Response({"detail": f"Upload processing failed: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class PageUploadView(APIView):
    """
    Receives an incremental single-page upload (DOCX or PDF) from the agent.
    Identifies the page number from the request and updates the PageVersion ledger.
    """
    permission_classes = [permissions.IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    @staticmethod
    def _to_int(value):
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _resolve_processing_upload_file(bundle, page_number, uploaded_file):
        """
        Prefer the current request upload, but fall back to the latest stored
        desktop page upload so later sync operations keep the real agent file.
        """
        if uploaded_file:
            return uploaded_file

        version = (
            PageVersion.objects.filter(
                document_id=bundle.document_id,
                page_number=page_number,
                is_valid=True,
            )
            .select_related("uploaded_pdf")
            .order_by("slice_size", "-updated_at", "id")
            .first()
        )
        if not version or not version.uploaded_pdf or not version.uploaded_pdf.file:
            return None

        try:
            version.uploaded_pdf.file.open("rb")
            version.uploaded_pdf.file.seek(0)
        except Exception:
            pass
        return version.uploaded_pdf.file

    @staticmethod
    def _sync_processing_submission(user, bundle, page_number, uploaded_file, bundle_version_id=None) -> bool:
        """
        Bridge desktop incremental uploads into the processing pipeline:
        mark matching PageAssignment as SUBMITTED and create/update SubmittedPage.
        Accepts re-uploads from the same device (no version locking).
        """
        from apps.processing.models import PageAssignment, SubmittedPage
        from apps.processing.services.core import ProcessingService
        from common.enums import PageAssignmentStatus, UserRole

        assignments_qs = PageAssignment.objects.filter(
            document_id=bundle.document_id,
            page__page_number=page_number,
        ).order_by("-assigned_at")

        # Prefer active assignments first; fallback to latest submitted row.
        assignment = assignments_qs.filter(
            status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
        ).first() or assignments_qs.filter(
            status=PageAssignmentStatus.SUBMITTED
        ).first()

        if not assignment:
            logger.warning(
                "No matching PageAssignment found for desktop upload: "
                f"doc={bundle.document_id}, page={page_number}, user={user.id}"
            )
            return False

        # Use the assignment owner for submission bookkeeping.
        assignment_user = (
            assignment.resource.user
            if assignment.resource and getattr(assignment.resource, "user", None)
            else user
        )
        source_file = PageUploadView._resolve_processing_upload_file(bundle, page_number, uploaded_file)

        if assignment.status in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]:
            ProcessingService.complete_assignment(
                assignment.id,
                assignment_user,
                uploaded_file=source_file,
                bundle=bundle,
                bundle_version_id=bundle_version_id,
            )
            return True

        if assignment.status == PageAssignmentStatus.SUBMITTED:
            submission = SubmittedPage.objects.filter(assignment=assignment).first()
            if submission and source_file:
                from pathlib import Path
                try:
                    source_file.seek(0)
                except Exception:
                    pass
                source_name = Path(str(getattr(source_file, "name", "") or "")).name or f"page_{page_number}.docx"
                submission.output_page_file.save(source_name, source_file, save=True)
            return True

        # Admin desktop tokens can still force completion of an assignment in edge cases.
        if user.role == UserRole.ADMIN or user.is_superuser or user.is_staff:
            ProcessingService.complete_assignment(assignment.id, user, uploaded_file=source_file, bundle=bundle)
            return True

        logger.info(
            "Desktop upload received for non-active assignment state: "
            f"assignment={assignment.id}, status={assignment.status}"
        )
        return False

    def post(self, request, device_id, bundle_id):
        device = get_object_or_404(DesktopDevice, id=device_id, user=request.user, is_active=True)

        # Find the bundle by ID only — then verify/restore lease
        bundle = get_object_or_404(AssignmentBundle, id=bundle_id)

        # Auto-re-lease if the bundle belongs to this user but lost its lease
        if bundle.leased_to != device:
            # Verify user has assignments for this document
            from apps.processing.models import PageAssignment
            has_assignment = PageAssignment.objects.filter(
                resource__user=request.user,
                page__document=bundle.document,
            ).exists()
            if not has_assignment and not (request.user.is_superuser or getattr(request.user, 'role', None) == 'ADMIN'):
                return Response({"detail": "No assignments for this document."}, status=status.HTTP_403_FORBIDDEN)

            # Re-acquire the lease
            bundle.leased_to = device
            bundle.status = AssignmentBundle.Status.LEASED
            bundle.lease_expires_at = timezone.now() + timedelta(minutes=LEASE_MINUTES)
            bundle.save()
            logger.info(f"Auto re-leased bundle {bundle_id} to device {device_id} for upload.")

        uploaded_file = request.FILES.get("page_file")
        if not uploaded_file:
            return Response({"detail": "page_file is required"}, status=status.HTTP_400_BAD_REQUEST)

        bundle_page_number = self._to_int(request.data.get("bundle_page_number"))
        page_number = self._to_int(request.data.get("source_page_number"))
        legacy_page_number = self._to_int(request.data.get("page_number"))

        if page_number is None:
            page_number = legacy_page_number

        if page_number is None and bundle_page_number is not None:
            if bundle_page_number < 1 or bundle_page_number > len(bundle.page_numbers):
                return Response({"detail": "bundle_page_number is outside bundle page count"}, status=status.HTTP_400_BAD_REQUEST)
            page_number = int(bundle.page_numbers[bundle_page_number - 1])

        if page_number is None:
            return Response(
                {"detail": "Provide source_page_number or page_number or bundle_page_number"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if page_number not in set(bundle.page_numbers):
            return Response(
                {"detail": f"Page {page_number} is not part of bundle pages {bundle.page_numbers}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        bundle_version_id = request.data.get("bundle_version_id", "") or ""

        try:
            processing_synced = False
            with transaction.atomic():
                from .services import register_page_upload
                version = register_page_upload(bundle, page_number, uploaded_file, bundle_version_id=bundle_version_id)
                
                # Check coverage for this bundle (Internal metric)
                # But trigger global merge task
                task_deterministic_merge.delay(str(bundle.document_id))

            try:
                processing_synced = self._sync_processing_submission(
                    request.user,
                    bundle,
                    page_number,
                    uploaded_file,
                    bundle_version_id=bundle_version_id
                )
            except Exception as sync_exc:
                logger.exception(
                    "Desktop upload succeeded but processing sync failed "
                    f"for doc={bundle.document_id}, page={page_number}: {sync_exc}"
                )

            return Response({
                "ok": True,
                "page_number": page_number,
                "bundle_page_number": bundle_page_number,
                "version_id": str(version.id),
                "document_id": str(bundle.document_id),
                "processing_synced": processing_synced,
            })
        except Exception as e:
            logger.exception(f"Incremental upload failed for page {page_number} in bundle {bundle_id}")
            return Response({"detail": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class BundleProcessingSyncView(APIView):
    """
    Reconcile already-uploaded desktop pages with processing submissions.
    Useful when files were uploaded earlier but assignment state was not moved to SUBMITTED.
    """
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, device_id, bundle_id):
        device = get_object_or_404(DesktopDevice, id=device_id, user=request.user, is_active=True)
        bundle = get_object_or_404(AssignmentBundle, id=bundle_id, leased_to=device)

        uploaded_page_numbers = set(
            PageVersion.objects.filter(bundle=bundle).values_list("page_number", flat=True).distinct()
        )
        bundle_pages = [int(p) for p in (bundle.page_numbers or [])]

        synced_pages = []
        skipped_pages = []
        for page_number in bundle_pages:
            if page_number not in uploaded_page_numbers:
                skipped_pages.append(page_number)
                continue
            if PageUploadView._sync_processing_submission(request.user, bundle, page_number, None):
                synced_pages.append(page_number)
            else:
                skipped_pages.append(page_number)

        return Response(
            {
                "ok": True,
                "bundle_id": str(bundle.id),
                "document_id": str(bundle.document_id),
                "uploaded_pages": sorted(uploaded_page_numbers),
                "synced_pages": sorted(synced_pages),
                "skipped_pages": sorted(skipped_pages),
            }
        )


class MergeDashboardView(APIView):
    """
    Returns the current coverage and health of a document's reconstruction.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, document_id):
        from apps.documents.models import Document
        doc = get_object_or_404(Document, id=document_id)
        
        resolver = PageVersionResolver(doc)
        page_map = resolver.resolve()
        
        total_pages = doc.total_pages or 0
        coverage = []
        missing = []
        
        for p_num in range(1, total_pages + 1):
            version = page_map.get(p_num)
            if version:
                coverage.append({
                    "page_number": p_num,
                    "version_id": str(version.id),
                    "slice_size": version.slice_size,
                    "uploaded_at": version.updated_at,
                    "device": version.uploaded_pdf.device.device_name if version.uploaded_pdf.device else "Unknown"
                })
            else:
                missing.append(p_num)
        
        latest_manifest = MergeManifest.objects.filter(document=doc).order_by('-created_at').first()
        
        return Response({
            "document_id": str(doc.id),
            "total_pages": total_pages,
            "covered_count": len(coverage),
            "missing_count": len(missing),
            "is_complete": len(missing) == 0,
            "missing_pages": missing,
            "latest_manifest": {
                "id": str(latest_manifest.id) if latest_manifest else None,
                "status": latest_manifest.status if latest_manifest else "NONE",
                "completed_at": latest_manifest.completed_at
            } if latest_manifest else None,
            "coverage_map": coverage
        })


class MergeActionView(APIView):
    """
    Triggers a manual merge attempt for a document.
    """
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, document_id):
        from apps.documents.models import Document
        doc = get_object_or_404(Document, id=document_id)
        
        # We can trigger synchronously or via Celery. 
        # For small docs, we return immediate result.
        engine = ZeroLossMergeEngine(doc)
        manifest = engine.execute()
        
        if manifest.status == MergeManifest.Status.SUCCESS:
            return Response({
                "detail": "Merge successful",
                "manifest_id": str(manifest.id),
                "final_pdf": manifest.final_pdf.url if manifest.final_pdf else None
            })
        else:
            return Response({
                "detail": "Merge failed or incomplete",
                "status": manifest.status,
                "missing_pages": manifest.missing_pages,
                "error": manifest.error_details
            }, status=status.HTTP_422_UNPROCESSABLE_ENTITY)


class BundleAdminActionView(APIView):
    """
    Handles Admin Approval / Rejection of Submitted Bundles.
    Enforces strict bundle-based pipeline for Job projects.
    """
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def post(self, request, bundle_id):
        action = request.data.get("action")
        reason = request.data.get("reason", "")

        if action not in ["approve", "reject"]:
            return Response({"detail": "Action must be approve or reject"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            with transaction.atomic():
                bundle = AssignmentBundle.objects.select_for_update().get(id=bundle_id)
                
                if bundle.status != AssignmentBundle.Status.SUBMITTED:
                    return Response({"detail": f"Bundle is in status {bundle.status}, cannot review."}, status=status.HTTP_400_BAD_REQUEST)

                bundle.reviewed_by = request.user
                bundle.reviewed_at = timezone.now()

                if action == "approve":
                    bundle.status = AssignmentBundle.Status.APPROVED
                    bundle.save()
                    logger.info(f"Admin {request.user} APPROVED bundle {bundle_id}")
                    
                    # Check if document is now ready for merge
                    trigger_merge_if_ready(bundle.document_id)
                    
                else: # reject
                    bundle.status = AssignmentBundle.Status.REJECTED
                    bundle.rejection_reason = reason
                    bundle.attempt_count += 1
                    bundle.save()
                    logger.warning(f"Admin {request.user} REJECTED bundle {bundle_id}: {reason}")
                    
                    # Reuse existing reassignment logic
                    reassign_bundle(bundle_id, request.user)

                return Response({"ok": True, "new_status": bundle.status})

        except AssignmentBundle.DoesNotExist:
            return Response({"detail": "Bundle not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            logger.exception(f"Admin review failed for bundle {bundle_id}")
            return Response({"detail": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class BundleStatusView(APIView):
    """
    Returns the current status of an AssignmentBundle.
    Called by the Desktop Agent after uploading pages to poll for review state.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, device_id, bundle_id):
        device = get_object_or_404(DesktopDevice, id=device_id, user=request.user, is_active=True)
        bundle = get_object_or_404(AssignmentBundle, id=bundle_id)

        return Response({
            "bundle_id": str(bundle.id),
            "status": bundle.status,
            "bundle_status": bundle.status,
            "document_id": str(bundle.document_id),
            "uploaded_at": bundle.uploaded_at,
            "lease_expires_at": bundle.lease_expires_at,
        })
