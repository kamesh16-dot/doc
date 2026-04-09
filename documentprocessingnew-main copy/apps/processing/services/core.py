import os
import logging
import json
import itertools
from datetime import timedelta
from decimal import Decimal
from django.db import models
from django.db import transaction
from django.db.models import Count, Q, F, Sum, FloatField
from django.db.models.functions import Coalesce
from django.utils import timezone
from django.core.files.base import ContentFile

from apps.accounts.models import ResourceProfile
from apps.documents.models import Document, Page
from apps.processing.models import PageAssignment
from apps.audit.models import AuditLog
from common.enums import (
    PageStatus, PageAssignmentStatus, DocumentStatus, 
    AssignmentStatus, PipelineStatus, AuditEventType, UserRole
)

logger = logging.getLogger(__name__)

class AssignmentService:
    TARGET_BLOCK_WEIGHT = 100.0 # Max complexity weight per assignment block (prevents over-splitting small docs)
    
    @staticmethod
    def get_available_resources():
        """
        Returns resources that can accept new work.
        Always queries DB fresh — never uses cached queryset.
        Annotates with computed current_load for ordering.
        """
        from common.enums import ResourceStatus, PageAssignmentStatus
        
        # Only assign to resources active within the last 5 minutes (session threshold)
        now = timezone.now()
        presence_threshold = now - timedelta(minutes=5)
        online_threshold = now - timedelta(seconds=ResourceProfile.ONLINE_THRESHOLD_SECONDS)
        
        return ResourceProfile.objects.filter(
            status=ResourceStatus.ACTIVE,
            is_available=True,
            last_seen__gte=online_threshold
        ).annotate(
            # Compute load directly in SQL
            computed_load=Coalesce(
                Sum(
                    'page_assignments__page__complexity_weight',
                    filter=Q(
                        page_assignments__status__in=[
                            PageAssignmentStatus.ASSIGNED, 
                            PageAssignmentStatus.IN_PROGRESS
                        ]
                    )
                ),
                0.0,
                output_field=FloatField()
            ),
            # Annotation for true online presence (heartbeat within 30s)
            is_strictly_online=models.Case(
                models.When(last_seen__gte=online_threshold, then=models.Value(True)),
                default=models.Value(False),
                output_field=models.BooleanField()
            )
        ).filter(
            computed_load__lt=models.F('max_capacity')
        ).order_by('-is_strictly_online', '-last_seen', '-max_capacity')

    @staticmethod
    def calculate_pages_for_resource(resource, available_pages, current_load=None):
        """
        Calculate how many pages this resource can accept
        based on their remaining capacity and page weights.
        Implements a greedy weighted fit (Section 5).
        """
        if current_load is None:
            resource.refresh_from_db()
            current_load = resource.current_load

        remaining_capacity = max(0.0, float(resource.max_capacity) - current_load)
        if remaining_capacity <= 0:
            return []

        # Sort available pages by weight DESCENDING (Greedy approach)
        # to ensure complex pages are handled first and balanced across resources.
        sorted_pages = sorted(
            available_pages, 
            key=lambda p: p.complexity_weight or 1.0, 
            reverse=True
        )

        allocated    = []
        total_weight = 0.0

        for page in sorted_pages:
            page_weight = page.complexity_weight or 1.0
            
            if total_weight + page_weight <= min(
                AssignmentService.TARGET_BLOCK_WEIGHT,
                remaining_capacity
            ):
                allocated.append(page)
                total_weight += page_weight
            
            # If we hit capacity, we stop trying to fit more in this cycle
            if total_weight >= min(AssignmentService.TARGET_BLOCK_WEIGHT, remaining_capacity):
                break

        return allocated

    @staticmethod
    def validate_document_integrity(document):
        """
        Technical Integrity Check: Compare stored Page count against original PDF metadata.
        Ensures strict sequential order and file existence.
        """
        from apps.documents.models import Page
        from common.enums import DocumentStatus, PipelineStatus
        
        expected_count = document.total_pages
        actual_pages = Page.objects.filter(document=document).order_by('page_number')
        actual_count = actual_pages.count()

        if expected_count is not None and actual_count != expected_count:
            error_msg = f"Integrity Breach: Expected {expected_count} pages, found {actual_count}."
            document.status = DocumentStatus.FAILED
            document.pipeline_status = PipelineStatus.FAILED
            document.pipeline_error = error_msg
            document.save()
            logger.error(f"Integrity check failed for {document.id}: {error_msg}")
            return False, error_msg

        # Check for sequence gaps and file presence
        for idx, page in enumerate(actual_pages):
            expected_num = idx + 1
            if page.page_number != expected_num:
                error_msg = f"Integrity Breach: Sequence gap at page {expected_num}. Found {page.page_number}."
                return False, error_msg
            
            if not page.content_file or not os.path.exists(page.content_file.path):
                error_msg = f"Integrity Breach: Missing file for page {page.page_number}."
                return False, error_msg

        return True, "Integrity verified."

    @staticmethod
    def _broadcast_resource_update(resource):
        """
        Helper to broadcast resource status via WebSockets.
        """
        try:
            from channels.layers import get_channel_layer
            from asgiref.sync import async_to_sync
            channel_layer = get_channel_layer()
            if channel_layer:
                async_to_sync(channel_layer.group_send)(
                    'admin_notifications',
                    {
                        'type': 'resource_status_update',
                        'payload': {
                            'resource_id':   resource.pk,
                            'username':      resource.user.username,
                            'status':        resource.status,
                            'current_load':  round(resource.current_load, 2),
                            'max_capacity':  resource.max_capacity,
                            'remaining':     round(resource.remaining_capacity, 2),
                        }
                    }
                )
        except: pass

    @staticmethod
    @transaction.atomic
    def assign_pages_to_resource(page_ids, resource, broadcast=True):
        """
        Atomically assign pages to a resource.
        Validates resource status and capacity INSIDE transaction
        to prevent race conditions.
        """
        from rest_framework.exceptions import ValidationError
        from common.enums import ResourceStatus, PageStatus, PageAssignmentStatus, AuditEventType

        # Lock resource row to prevent concurrent over-assignment
        resource = ResourceProfile.objects.select_for_update().get(
            pk=resource.pk
        )

        # Re-validate INSIDE transaction
        if resource.status != ResourceStatus.ACTIVE:
            raise ValidationError({
                'error':   True,
                'code':    'RESOURCE_INACTIVE',
                'message': f'Resource {resource.user.username} '
                           f'is {resource.status}',
            })

        current_load = resource.current_load
        pages_to_assign = Page.objects.filter(
            pk__in=page_ids
        ).select_for_update()

        # Validate total weight fits in remaining capacity
        total_new_weight = sum(
            (p.complexity_weight or 1.0) for p in pages_to_assign
        )

        if current_load + total_new_weight > resource.max_capacity:
            raise ValidationError({
                'error':   True,
                'code':    'RESOURCE_AT_CAPACITY',
                'message': f'Assignment would exceed capacity. '
                           f'Remaining: {resource.remaining_capacity:.1f}, '
                           f'Requested weight: {total_new_weight:.1f}',
            })

        assignments = []
        for page in pages_to_assign:
            # Validate page is not already assigned
            if page.status in [PageStatus.ASSIGNED, PageStatus.IN_PROGRESS]:
                continue  # skip already-assigned pages silently

            # ── REASSIGNMENT LOGIC (Anti-Loop) ─────────────────
            # Find the most recent terminal assignment for this page to inherit count
            last_asgn = PageAssignment.objects.filter(page=page).order_by('-assigned_at').first()
            
            re_count = 0
            is_re = False
            prev_asgn = None
            
            if last_asgn:
                re_count = (last_asgn.reassignment_count or 0) + 1
                is_re = True
                prev_asgn = last_asgn

            assignment = PageAssignment.objects.create(
                page=page,
                resource=resource,
                document=page.document,
                status=PageAssignmentStatus.ASSIGNED,
                max_processing_time=600,  # ✅ Fixed to 10 minutes (600s) as requested
                reassignment_count=re_count,
                is_reassigned=is_re,
                reassigned_from=prev_asgn
            )
            page.status = PageStatus.ASSIGNED
            page.current_assignee = resource.user
            page.is_locked = True
            page.save(update_fields=['status', 'current_assignee', 'is_locked'])
            
            # Audit
            AuditLog.objects.create(
                action=AuditEventType.ASSIGNED if not is_re else AuditEventType.REASSIGNED,
                assignment_id=assignment.id,
                document_id=page.document.id,
                actor=None,
                metadata={
                    'resource_id': resource.id, 
                    'page_number': page.page_number,
                    'is_reassignment': is_re,
                    'reassignment_count': re_count,
                    'previous_assignment_id': prev_asgn.id if prev_asgn else None
                }
            )
            assignments.append(assignment)

        # Refresh status after assignment
        resource.refresh_status()
        
        if broadcast:
            AssignmentService._broadcast_resource_update(resource)

        return assignments

    @staticmethod
    def assign_pages(document_id=None):
        """
        Main entry point for the assignment engine.
        Allocates pending pages to available resources.
        """
        from apps.processing.models import DocumentQueue
        from common.enums import QueueStatus, PageStatus, PipelineStatus

        with transaction.atomic():
            # 1. Get candidate documents from queue
            query = DocumentQueue.objects.select_for_update(skip_locked=True)
            if document_id:
                queue_items = query.filter(document_id=document_id)
            else:
                # Include ASSIGNED documents in case they have newly pending pages (e.g. from re-splitting)
                queue_items = query.filter(
                    status__in=[QueueStatus.WAITING, QueueStatus.ASSIGNED]
                ).order_by('position', 'created_at')

            total_assigned = 0
            
            for item in queue_items:
                doc = item.document
                
                # Fetch pending pages
                pending_pages = Page.objects.filter(
                    document=doc,
                    status__in=[PageStatus.PENDING, PageStatus.FAILED]
                ).order_by('page_number')
                
                if not pending_pages.exists():
                    item.status = QueueStatus.ASSIGNED
                    item.save(update_fields=['status'])
                    continue

            # 3. Get all resources once at the start of the cycle
            resources = list(AssignmentService.get_available_resources())
            if not resources:
                logger.info("No active resources available for assignment.")
                return 0

            # Global Page Pool: Collect ALL eligible pages from ALL docs in queue
            # to ensure we balance complexity across the entire workforce.
            global_page_pool = []
            doc_exclusion_map = {} # doc_id -> exclusion_map (page_id -> set(resource_ids))

            for item in queue_items:
                doc = item.document
                
                # ✅ Anti-Skip: Verify Technical Integrity BEFORE assignment
                is_valid, integrity_msg = AssignmentService.validate_document_integrity(doc)
                if not is_valid:
                    item.status = QueueStatus.FAILED
                    item.save()
                    continue

                # Pre-fetch all past failed assignments and manual exclusions for this document
                from apps.processing.models import RejectedPage
                doc_exclusions = {} # page_id -> set(resource_ids)
                
                # 1. Automatic exclusions from past failed assignments
                past_fails = PageAssignment.objects.filter(
                    document=doc,
                    status__in=[
                        PageAssignmentStatus.REASSIGNED, 
                        PageAssignmentStatus.TIMED_OUT, 
                        PageAssignmentStatus.QUALITY_FAILED
                    ]
                ).values('page_id', 'resource_id')
                
                for pf in past_fails:
                    pid, rid = pf['page_id'], pf['resource_id']
                    if rid:
                        doc_exclusions.setdefault(pid, set()).add(rid)

                # 2. Manual exclusions from RejectedPage records
                manual_excl = RejectedPage.objects.filter(document=doc).values('page_id', 'excluded_resources__id')
                for me in manual_excl:
                    pid, rid = me['page_id'], me['excluded_resources__id']
                    if rid:
                        doc_exclusions.setdefault(pid, set()).add(rid)
                
                doc_exclusion_map[doc.id] = doc_exclusions

                # ✅ Recovery: Clear stale locks for PENDING/FAILED pages
                # If a page is PENDING but is_locked=True, it's a stale lock from a crashed cycle.
                Page.objects.filter(
                    document=doc,
                    status__in=[PageStatus.PENDING, PageStatus.FAILED],
                    is_locked=True
                ).update(is_locked=False)

                # Fetch pending pages - STRICTLY filter for 'PENDING' or 'FAILED' (Unassigned)
                # ✅ Locked during fetch to prevent concurrent assignment threads from seeing the same pool.
                pending_pages = list(Page.objects.filter(
                    document=doc,
                    status__in=[PageStatus.PENDING, PageStatus.FAILED],
                    is_locked=False
                ).select_for_update(skip_locked=True).order_by('page_number'))
                
                if not pending_pages:
                    logger.info(f"Document {doc.id} ({doc.title}) has no pending pages. Marking queue ASSIGNED.")
                    item.status = QueueStatus.ASSIGNED
                    item.save(update_fields=['status'])
                    continue

                global_page_pool.extend(pending_pages)

            if not global_page_pool:
                return 0

            # ── BLOCK-BASED SEQUENTIAL ASSIGNMENT ───────────────────
            # Sort resources by current load (balance existing work)
            resources.sort(key=lambda r: r.computed_load)
            resource_loads = {r.id: r.computed_load for r in resources}
            updated_resources = set()

            for item in queue_items:
                doc = item.document
                # Get pending pages for this doc in sequential order
                doc_pages = sorted([p for p in global_page_pool if p.document_id == doc.id], key=lambda x: x.page_number)
                if not doc_pages: continue

                # Iterate through pages and group into blocks
                idx = 0
                while idx < len(doc_pages):
                    # Filter resources that have at least some capacity left
                    eligible_res = [
                        res for res in resources 
                        if resource_loads[res.id] < res.max_capacity
                    ]
                    if not eligible_res:
                        logger.warning(f"No capacity left for doc {doc.title} at page {doc_pages[idx].page_number}")
                        break
                    
                    # Sort candidates by lowest load
                    eligible_res.sort(key=lambda r: resource_loads[r.id])
                    
                    picked_res = None
                    block_pages = []
                    block_weight = 0

                    for res in eligible_res:
                        # Test if this resource can take at least the current page
                        p = doc_pages[idx]
                        w = p.complexity_weight or 1.0
                        exclusions = doc_exclusion_map.get(doc.id, {}).get(p.id, set())
                        
                        if res.id not in exclusions and (resource_loads[res.id] + w <= res.max_capacity):
                            picked_res = res
                            # Fill the block sequentially for THIS resource
                            while idx < len(doc_pages):
                                curr_p = doc_pages[idx]
                                curr_w = curr_p.complexity_weight or 1.0
                                curr_excl = doc_exclusion_map.get(doc.id, {}).get(curr_p.id, set())
                                
                                # Conditions to stop filling this block:
                                # 1. Resource hit capacity
                                # 2. Resource is excluded from this specific page
                                # 3. Block weight target exceeded (Section 11)
                                if res.id in curr_excl or (resource_loads[res.id] + block_weight + curr_w > res.max_capacity):
                                    break
                                
                                if block_pages and (block_weight + curr_w > AssignmentService.TARGET_BLOCK_WEIGHT):
                                    break
                                
                                block_pages.append(curr_p)
                                block_weight += curr_w
                                idx += 1
                            break # Picked the resource and filled as much as possible

                    if picked_res and block_pages:
                        try:
                            with transaction.atomic():
                                p_ids = [bp.id for bp in block_pages]
                                AssignmentService.assign_pages_to_resource(p_ids, picked_res, broadcast=False)
                                
                                resource_loads[picked_res.id] += block_weight
                                updated_resources.add(picked_res)
                                total_assigned += len(block_pages)
                        except Exception as e:
                            logger.error(f"Failed to assign block to res {picked_res.id}: {e}")
                            idx += 1 # Avoid infinite loop
                    else:
                        # Current page doc_pages[idx] cannot be assigned to ANY available resource
                        logger.warning(f"Page {doc_pages[idx].page_number} (doc {doc.id}) has no eligible resources.")
                        idx += 1

            # Check if documents in queue are now fully assigned
            for item in queue_items:
                doc = item.document
                if item.status == QueueStatus.WAITING:
                    still_pending = Page.objects.filter(
                        document=doc, status__in=[PageStatus.PENDING, PageStatus.FAILED]
                    ).exists()
                    if not still_pending:
                        item.status = QueueStatus.ASSIGNED
                        item.save(update_fields=['status'])
                        doc.pipeline_status = PipelineStatus.IN_PROGRESS
                        doc.save(update_fields=['pipeline_status'])
                        logger.info(f"Document {doc.title} fully assigned.")
            
            # Bulk broadcast for all updated resources
            for res in updated_resources:
                AssignmentService._broadcast_resource_update(res)

            if total_assigned > 0:
                logger.info(f"Assignment cycle completed. Total assigned: {total_assigned}")
                
            return total_assigned


    @staticmethod
    def reassign_rejected_assignment(assignment_id, manager_user, auto_assign=True):
        """
        Reassigns a REJECTED PageAssignment (Section 9).
        """
        from common.enums import RejectionReason
        
        with transaction.atomic():
            old_assignment = PageAssignment.objects.select_for_update().get(id=assignment_id)
            doc = old_assignment.document
            page = old_assignment.page
            
            # 1. Reset Page & Cancel Old Assignment
            old_assignment.status = PageAssignmentStatus.REASSIGNED
            old_assignment.save()

            page.status = PageStatus.PENDING
            page.current_assignee = None
            page.is_locked = False
            page.save(update_fields=['status', 'current_assignee', 'is_locked'])

            if not auto_assign:
                return None

            # 2. Get active candidates EXCLUDING ALL previous failed resources for this page
            from common.enums import ResourceStatus
            from django.core.cache import cache
            from apps.processing.models import RejectedPage
            
            # Use global PageAssignment for query
            excluded_resource_ids = set(PageAssignment.objects.filter(
                page=page,
                status__in=[
                    PageAssignmentStatus.REASSIGNED, 
                    PageAssignmentStatus.TIMED_OUT,
                    PageAssignmentStatus.QUALITY_FAILED
                ]
            ).values_list('resource_id', flat=True))
            
            # Also check RejectedPage exclusions (manually marked by admin)
            page_rejection_exclusions = RejectedPage.objects.filter(page=page).values_list('excluded_resources__id', flat=True)
            excluded_resource_ids.update(filter(None, page_rejection_exclusions))
            
            # Ensure the current resource being rejected is definitely excluded
            if old_assignment.resource_id:
                excluded_resource_ids.add(old_assignment.resource_id)
            
            candidates = list(ResourceProfile.objects.filter(
                status=ResourceStatus.ACTIVE,
                is_available=True,
                user__is_active=True
            ).exclude(id__in=list(excluded_resource_ids)).select_related('user'))
            
            logger.info(f"Reassigning Page {page.page_number}. Excluded IDs: {excluded_resource_ids}. Candidate count: {len(candidates)}")
            best_candidate = None
            max_score = -float('inf')
            
            # Source of truth for online status in DB
            now = timezone.now()
            online_limit = now - timedelta(seconds=ResourceProfile.ONLINE_THRESHOLD_SECONDS)

            for cand in candidates:
                is_online = cand.last_seen and cand.last_seen >= online_limit
                presence_bonus = 10000 if is_online else 0
                
                cap = cand.max_capacity - cand.current_load
                if cap > 0:
                    score = (cap * 2) - (cand.avg_processing_time * 0.5) - (cand.rejection_count * 3) + presence_bonus
                    if score > max_score:
                        max_score = score
                        best_candidate = cand
            
            # 2.5 Fallback: If no one has capacity, pick anyone Online, otherwise anyone available
            if not best_candidate:
                # Try finding any Online candidate first even if at capacity (to handle urgent reassignments)
                best_candidate = next((c for c in candidates if c.last_seen and c.last_seen >= online_limit), None)
                
            if not best_candidate:
                # Absolute fallback: First available candidate
                best_candidate = next(iter(candidates), None)

            if not best_candidate:
                # Add back into queue for assign_pages task to pick up later
                return None

            # 3. Create new assignment
            new_assign = PageAssignment.objects.create(
                page=page,
                resource=best_candidate,
                document=doc,
                status=PageAssignmentStatus.ASSIGNED,
                is_reassigned=True,
                reassignment_count=old_assignment.reassignment_count + 1,
                reassigned_from=old_assignment,
                max_processing_time=600  # ✅ Fixed to 10 minutes (600s) as requested
            )
            
            # Update page back to ASSIGNED with new user
            page.status = PageStatus.ASSIGNED
            page.current_assignee = best_candidate.user
            page.save()
            
            # 4. Create ReassignmentLog audit trail
            from apps.processing.models import ReassignmentLog
            ReassignmentLog.objects.create(
                original_assignment=old_assignment,
                new_assignment=new_assign,
                reassigned_by=manager_user,
                reason=RejectionReason.QUALITY_FAIL, # Defaults
                previous_resource=old_assignment.resource,
                new_resource=best_candidate
            )
            
            return new_assign


class ProcessingService:

    @staticmethod
    def complete_assignment(assignment_id, resource_user, uploaded_file=None, bundle=None, bundle_version_id=None):
        """
        Marks a PageAssignment as SUBMITTED (Section 4).
        Creates a SubmittedPage record for Admin Review.
        """
        from apps.processing.models import SubmittedPage
        from common.enums import ReviewStatus
        
        with transaction.atomic():
            # 1. Lock and Validate
            try:
                assignment = PageAssignment.objects.select_for_update().get(id=assignment_id)
            except PageAssignment.DoesNotExist:
                raise ValueError("Assignment not found")

            is_admin = resource_user.role == UserRole.ADMIN or resource_user.is_superuser or resource_user.is_staff
            if assignment.resource.user != resource_user and not is_admin:
                raise PermissionError("User is not the assignee of this page")
            
            if assignment.status not in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]:
                 raise ValueError("Assignment is not ACTIVE")

            page = assignment.page
            
            # 2. Update Page Status (HTML Text is already saved via WebSocket)
            now = timezone.now()
            page.status = PageStatus.SUBMITTED
            page.current_assignee = None
            page.is_locked = False
            page.processing_completed_at = now
            page.processing_end_date = now.date()
            page.processing_end_time = now.time()
            if not page.processing_started_at:
                page.processing_started_at = now
                page.processing_start_date = now.date()
                page.processing_start_time = now.time()
            page.save()

            # 3. Update Assignment Status
            assignment.status = PageAssignmentStatus.SUBMITTED
            assignment.submitted_at = now
            assignment.processing_end_at = now
            if not assignment.processing_start_at:
                assignment.processing_start_at = now
            assignment.save()
            
            # 4. Record SubmittedPage for Review
            submission, created = SubmittedPage.objects.update_or_create(
                assignment=assignment,
                defaults={
                    'page': page,
                    'document': assignment.document,
                    'bundle': bundle,
                    'bundle_version_id': bundle_version_id,
                    'submitted_by': resource_user,
                    'page_number': page.page_number,
                    'final_text': page.text_content or "",
                    'processing_duration': assignment.processing_duration,
                    'processing_start_at': assignment.processing_start_at,
                    'processing_end_at': assignment.processing_end_at,
                    'review_status': ReviewStatus.PENDING_REVIEW
                }
            )

            try:
                if uploaded_file:
                    from pathlib import Path
                    try:
                        uploaded_file.seek(0)
                    except Exception:
                        pass
                    submitted_name = Path(str(getattr(uploaded_file, "name", "") or "")).name or f"submitted_page_{page.page_number}_{assignment.id}.docx"
                    submission.output_page_file.save(submitted_name, uploaded_file, save=True)
                else:
                    # Fallback to generated page snapshot when no file is provided.
                    from apps.processing.services.pdf_baking import PDFBakeService
                    baked_content = PDFBakeService.bake_page_edits(page)
                    filename = f"submitted_page_{page.page_number}_{assignment.id}.pdf"
                    submission.output_page_file.save(filename, ContentFile(baked_content), save=True)
            except Exception as pdf_err:
                logger.error(f"Error generating submitted page artifact: {pdf_err}")

                
            # 5. Audit Log
            AuditLog.objects.create(
                action=AuditEventType.COMPLETED,
                assignment_id=assignment.id,
                document_id=assignment.document.id,
                actor=resource_user,
                new_status=AssignmentStatus.COMPLETED, # Meta enum
                metadata={
                    'duration_sec': assignment.processing_duration.total_seconds() if assignment.processing_duration else 0
                }
            )

            # Signal handler check_all_pages_submitted triggers automatically on SubmittedPage save
            # Resource current_load drops automatically via PageAssignment status change signal
            
            # Post-commit schedule assign pages
            from apps.processing.tasks import assign_pages_task
            transaction.on_commit(lambda: assign_pages_task.delay())
