from rest_framework import viewsets, permissions, status, views
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.decorators import action
import logging
from django.utils import timezone
from django.shortcuts import get_object_or_404
from django.db import transaction
import uuid

from apps.documents.models import Document, Page, PageTable, PageImage
from apps.accounts.models import ResourceProfile
from apps.processing.models import PageAssignment, SubmittedPage, ReassignmentLog
from apps.processing.serializers import (
    PageAssignmentSerializer, SubmittedPageSerializer, 
    StartProcessingSerializer, SubmitProcessingSerializer
)
from apps.documents.serializers import BlockSerializer, PageTableSerializer, PageImageSerializer
from apps.processing.services.core import AssignmentService, ProcessingService
from common.enums import (
    PageAssignmentStatus, UserRole, ResourceStatus, 
    PipelineStatus, ReviewStatus, PageStatus, DocumentStatus
)
from rest_framework.decorators import api_view, permission_classes, authentication_classes
from django.views.decorators.csrf import csrf_exempt
from rest_framework.authentication import SessionAuthentication, TokenAuthentication

class CsrfExemptSessionAuthentication(SessionAuthentication):
    def enforce_csrf(self, request):
        return  # Skip CSRF check for this specific authentication class

logger = logging.getLogger(__name__)

class WorkspaceViewSet(viewsets.ViewSet):
    """
    Section 11 APIs for the Resource Workspace.
    Using `doc_ref` and `page_number` directly instead of DB IDs.
    """
    permission_classes = [permissions.IsAuthenticated]

    
    @action(detail=False, methods=['post'], url_path=r'pages/(?P<page_id>\d+)/blocks/create')
    def create_block(self, request, page_id=None):
        """Manually create a new block or table from the editor."""
        page = get_object_or_404(Page, id=page_id)
        data = request.data
        
        from apps.documents.models import Block, PageTable
        import uuid
        
        block_type = data.get('type', 'text')
        table_ref = None

        with transaction.atomic():
            block = Block.objects.create(
                page=page,
                block_id=f"manual_{uuid.uuid4().hex[:8]}",
                block_type=block_type,
                is_underlined=data.get('is_underlined', False),
                x=data.get('x', 0),
                y=data.get('y', 0),
                width=data.get('width', 0),
                height=data.get('height', 0),
                bbox=[data.get('x', 0), data.get('y', 0), data.get('x', 0) + data.get('width', 0), data.get('y', 0) + data.get('height', 0)],
                extracted_text="",
                current_text="",
                last_edited_by=request.user,
                last_edited_at=timezone.now()
            )

            # If it's a table, create the structural PageTable record too
            if block_type == 'table':
                table_ref = f"table_{page_id}_{uuid.uuid4().hex[:6]}"
                
                # Intelligent Grid Detection
                rows = data.get('row_count')
                cols = data.get('col_count')
                
                if rows is None or cols is None:
                    from apps.processing.services.layout_engine import PDFLayoutEngine
                    engine = PDFLayoutEngine()
                    if page.content_file:
                        rows, cols = engine.detect_grid_in_area(
                            page.content_file.path, 
                            0, 
                            [block.x, block.y, block.x + block.width, block.y + block.height]
                        )
                    else:
                        rows, cols = 2, 2
                
                rows = int(rows or 2)
                cols = int(cols or 2)
                
                # Initialize JSON grid with empty cell objects
                grid = [[{'text': '', 'indent': 0} for _ in range(cols)] for _ in range(rows)]
                col_widths = [100.0 / cols] * cols
                row_heights = [100.0 / rows] * rows

                PageTable.objects.create(
                    page=page,
                    table_ref=table_ref,
                    x=block.x,
                    y=block.y,
                    width=block.width,
                    height=block.height,
                    row_count=rows,
                    col_count=cols,
                    table_json=grid,
                    col_widths=col_widths,
                    row_heights=row_heights,
                    is_manually_edited=True
                )
                
            # If it's an image area, create the PageImage record too
            if block_type == 'image':
                from apps.documents.models import PageImage
                image_ref = f"IMG_{block.block_id}"
                PageImage.objects.create(
                    page=page,
                    image_ref=image_ref,
                    x=block.x,
                    y=block.y,
                    width=block.width,
                    height=block.height
                )
        
        return Response({
            'status': 'Block created', 
            'block_id': str(block.id),
            'table_ref': table_ref
        })

    def _get_assignment(self, doc_ref, page_number, user):
        """Helper to fetch the relevant assignment for this user, including non-active ones."""
        is_admin = user.role == UserRole.ADMIN or user.is_superuser or user.is_staff
        
        # Allow both admins AND resources to see SUBMITTED/APPROVED if it's theirs
        status_filter = [
            PageAssignmentStatus.ASSIGNED, 
            PageAssignmentStatus.IN_PROGRESS,
            PageAssignmentStatus.SUBMITTED,
            PageAssignmentStatus.APPROVED
        ]

        try:
            filters = {
                'document__doc_ref': doc_ref,
                'page__page_number': page_number,
                'status__in': status_filter
            }
            if not is_admin:
                filters['resource__user'] = user
                
            assignment = PageAssignment.objects.filter(
                **filters
            ).select_related('document', 'page', 'resource__user').latest('assigned_at')
            
            if assignment.resource.user != user and not is_admin:
                raise PermissionError("Access denied")
                
            return assignment
        except PageAssignment.DoesNotExist:
            if is_admin:
                page = get_object_or_404(Page, document__doc_ref=doc_ref, page_number=page_number)
                return PageAssignment(document=page.document, page=page, status=PageAssignmentStatus.IN_PROGRESS)
            raise
            
    def _get_or_seed_blocks(self, page):
        """
        No longer seeds on the fly; extraction is pre-computed by Celery.
        This just returns the pre-extracted blocks.
        """
        from apps.documents.models import Block
        blocks = page.blocks.all().order_by('y', 'x')
        if not blocks.exists() and page.blocks_extracted:
            # If for some reason extraction flag is set but blocks are missing,
            # it might be an extraction failure or race condition.
            logger.warning(f"Page {page.id} marked as extracted but has no blocks.")
        return blocks
    
    def list(self, request):
        """List active assignments for the current resource, grouped by document."""
        assignments = PageAssignment.objects.filter(
            resource__user=request.user,
            status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
        ).select_related('document', 'page').order_by('document_id', 'page__page_number')
        
        grouped_data = {}
        for a in assignments:
            doc_id = a.document_id
            if doc_id not in grouped_data:
                grouped_data[doc_id] = {
                    'document_ref': a.document.doc_ref,
                    'document_title': a.document.title,
                    'document_total_pages': a.document.total_pages,
                    'pages': [],
                    'status': a.status,
                    'assigned_at': a.assigned_at
                }
            grouped_data[doc_id]['pages'].append(a.page.page_number)
            if a.status == PageAssignmentStatus.IN_PROGRESS:
                grouped_data[doc_id]['status'] = PageAssignmentStatus.IN_PROGRESS

        results = []
        for doc_id, data in grouped_data.items():
            pages = sorted(data['pages'])
            page_range = f"Pages {pages[0]}-{pages[-1]}" if len(pages) > 1 else f"Page {pages[0]}"
            
            results.append({
                'id': doc_id,
                'document_ref': data['document_ref'],
                'document_title': data['document_title'],
                'document_total_pages': data['document_total_pages'],
                'page_number': pages[0],
                'page_range': page_range,
                'total_pages': len(pages),
                'status': data['status'],
                'status_display': data['status'].replace('_', ' ').title(),
                'assigned_at': data['assigned_at']
            })

        return Response(results)
    
    @action(detail=False, methods=['get'])
    def history(self, request):
        """List submitted pages for the current resource"""
        submissions = SubmittedPage.objects.filter(
            submitted_by=request.user
        ).select_related('document', 'page').order_by('-submitted_at')
        
        serializer = SubmittedPageSerializer(submissions, many=True)
        return Response(serializer.data)

    @action(detail=False, methods=['post'], url_path=r'content/(?P<doc_ref>[^/]+)/(?P<page_number>\d+)/export')
    def export_content(self, request, doc_ref=None, page_number=None):
        """Generates a Word (.docx) file of the current workspace state (restricted to visible block)."""
        from apps.processing.services.export import ExportService
        from apps.processing.models import PageAssignment, PageAssignmentStatus
        from apps.documents.models import Page
        from django.http import FileResponse
        
        try:
            is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
            target_assignment = self._get_assignment(doc_ref, int(page_number), request.user)
            document = target_assignment.document
            
            # Determine pages to export (Must match get_workspace_data logic)
            show_full_doc = is_admin and request.query_params.get('block_view') != 'true' and request.data.get('scope') == 'all'
            
            if show_full_doc:
                export_pages = Page.objects.filter(document=document).order_by('page_number')
            else:
                target_page = Page.objects.get(document=document, page_number=page_number)
                block_assignment = PageAssignment.objects.filter(
                    document=document,
                    page=target_page
                ).order_by('-assigned_at').first()
                
                if block_assignment:
                    if block_assignment.status in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]:
                        status_group = [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                    elif block_assignment.status == PageAssignmentStatus.SUBMITTED:
                        status_group = [PageAssignmentStatus.SUBMITTED]
                    elif block_assignment.status == PageAssignmentStatus.APPROVED:
                        status_group = [PageAssignmentStatus.APPROVED]
                    else:
                        status_group = [block_assignment.status]

                    block_pages_ids = list(PageAssignment.objects.filter(
                        document=document,
                        resource=block_assignment.resource,
                        status__in=status_group
                    ).values_list('page_id', flat=True))
                    export_pages = Page.objects.filter(id__in=block_pages_ids).order_by('page_number')
                else:
                    export_pages = Page.objects.filter(id=target_page.id)

            if not export_pages.exists():
                export_pages = Page.objects.filter(document=document, page_number=page_number)
            
            # 3. Generate Word doc for these specific pages
            docx_buffer = ExportService.generate_word_export(document, include_unapproved=True, pages=export_pages)
            
            import re
            clean_name = re.sub(r'[^a-zA-Z0-9_-]', '_', (document.doc_ref or document.title or "export"))
            return FileResponse(
                docx_buffer, 
                as_attachment=True, 
                filename=f"{clean_name}.docx",
                content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.error(f"Word export failed: {e}", exc_info=True)
            return Response({
                'error': f"{type(e).__name__}: {str(e)}",
                'traceback': tb
            }, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=False, methods=['get'], url_path='content') # Keep for router listing
    def get_workspace_data(self, request, doc_ref=None, page_number=None):
        try:
            is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
            target_assignment = self._get_assignment(doc_ref, page_number, request.user)
            
            # 1. Fetch relevant pages based on role
            # For resources, only show THEIR assigned pages (Block Isolation)
            # For admins, show the full document context UNLESS 'block_view=true' is passed
            show_full_doc = is_admin and request.query_params.get('block_view') != 'true'
            
            if show_full_doc:
                all_pages = Page.objects.filter(document__doc_ref=doc_ref).order_by('page_number')
            else:
                # 1. Identify all pages in the block containing the target page
                # Find the assignment that covers the target page number
                target_page = Page.objects.get(document__doc_ref=doc_ref, page_number=page_number)
                
                # Find the assignment controlling this page (latest one)
                block_assignment = PageAssignment.objects.filter(
                    document__doc_ref=doc_ref,
                    page=target_page
                ).order_by('-assigned_at').first()
                
                if block_assignment:
                    # Identify all pages in THIS specific assignment block (same resource, same doc, same assignment batch)
                    # For simplicity, we define a block as pages assigned to the SAME resource on this document 
                    # that were active around the same time.
                    # Actually, our PageAssignment grouped by resource in the trace is what the user is looking for.
                    
                    # Define status clusters for grouping
                    if block_assignment.status in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]:
                        status_group = [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                    elif block_assignment.status == PageAssignmentStatus.SUBMITTED:
                        status_group = [PageAssignmentStatus.SUBMITTED]
                    elif block_assignment.status == PageAssignmentStatus.APPROVED:
                        status_group = [PageAssignmentStatus.APPROVED]
                    else:
                        status_group = [block_assignment.status]

                    block_pages_ids = list(PageAssignment.objects.filter(
                        document=block_assignment.document,
                        resource=block_assignment.resource,
                        status__in=status_group
                    ).values_list('page_id', flat=True))
                    
                    all_pages = Page.objects.filter(id__in=block_pages_ids).order_by('page_number')
                else:
                    all_pages = Page.objects.filter(id=target_page.id)
            
            if not all_pages.exists():
                 all_pages = Page.objects.filter(document__doc_ref=doc_ref, page_number=page_number)
            
            # 2. Fetch all assignments for this document to check ownership
            all_doc_assignments = PageAssignment.objects.filter(
                document__doc_ref=doc_ref,
                status__in=[
                    PageAssignmentStatus.ASSIGNED, 
                    PageAssignmentStatus.IN_PROGRESS, 
                    PageAssignmentStatus.SUBMITTED,
                    PageAssignmentStatus.APPROVED
                ]
            ).select_related('page', 'resource__user')
            
            # Map assignments by page number for quick lookup
            assignment_map = {a.page.page_number: a for a in all_doc_assignments}
            
            pages_data = []
            from apps.processing.services.nlp_engine import NLPInspector
            
            for p in all_pages:
                assignment = assignment_map.get(p.page_number)
                is_assigned_to_me = assignment and assignment.resource.user == request.user
                
                # Determine readability/editability
                if is_admin:
                    # Admins can edit anything that's not submitted/approved 
                    page_readonly = False 
                else:
                    # Resources can ONLY edit pages assigned to them that are active
                    if is_assigned_to_me:
                        page_readonly = assignment.status not in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                    else:
                        page_readonly = True # All other pages in the PDF are read-only context
                
                suggestions = []
                # Performance: Only run NLP analysis for editable pages or admins
                if is_admin or (is_assigned_to_me and not page_readonly):
                    try:
                        suggestions = NLPInspector.analyze_page_structure(p)
                    except: pass
                        
                blocks = self._get_or_seed_blocks(p)
                blocks_data = BlockSerializer(blocks, many=True).data
                
                tables = p.tables.all()
                tables_data = PageTableSerializer(tables, many=True).data
                
                images = p.images.all()
                images_data = PageImageSerializer(images, many=True, context={'request': request}).data

                pages_data.append({
                    'id': p.id,
                    'page_number': p.page_number,
                    'text_content': p.text_content,
                    'layout_data': p.layout_data,
                    'blocks': blocks_data,
                    'tables': tables_data,
                    'images': images_data,
                    'suggestions': suggestions,
                    'image_url': request.build_absolute_uri(p.content_file.url) if p.content_file else None,
                    'assignment_id': assignment.id if assignment else None,
                    'assignment_status': assignment.status if assignment else 'UNASSIGNED',
                    'is_readonly': page_readonly,
                    'is_assigned_to_me': is_assigned_to_me
                })


            return Response({
                'document': {
                    'name': target_assignment.document.name,
                    'doc_ref': doc_ref,
                    'total_pages': all_pages.count(),
                    'document_url': request.build_absolute_uri(target_assignment.document.file.url) if target_assignment.document.file else None
                },
                'pages': pages_data,
                'is_block': len(pages_data) > 1,
                'view_mode': 'ADMIN' if is_admin else 'RESOURCE'
            })
            
        except PageAssignment.DoesNotExist:
            return Response({'error': True, 'code': 'NOT_FOUND', 'message': f'Assignment not found for {doc_ref} page {page_number}'}, status=404)
        except PermissionError as e:
            return Response({'error': True, 'code': 'FORBIDDEN', 'message': str(e)}, status=403)
        except Exception as e:
            import traceback
            logger.error(f"Workspace API Error: {e}\n{traceback.format_exc()}")
            return Response({'error': True, 'code': 'SERVER_ERROR', 'message': str(e)}, status=500)

    @action(detail=False, methods=['get'], url_path=r'content/(?P<doc_ref>[^/.]+)/(?P<page_number>\d+)/preview')
    def preview_baked_pdf(self, request, doc_ref=None, page_number=None):
        """Returns the baked PDF for a specific page with current edits."""
        try:
            is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
            page = get_object_or_404(Page, document__doc_ref=doc_ref, page_number=page_number)

            if not is_admin:
                # Ensure the resource user has/had this page assigned and submitted/active
                has_access = PageAssignment.objects.filter(
                    page=page,
                    resource__user=request.user,
                    status__in=[
                        PageAssignmentStatus.ASSIGNED,
                        PageAssignmentStatus.IN_PROGRESS,
                        PageAssignmentStatus.SUBMITTED,
                        PageAssignmentStatus.APPROVED
                    ]
                ).exists()
                if not has_access:
                    return Response({'error': 'Permission denied. You can only view snapshots of your own submitted pages.'}, status=403)

            from apps.processing.services.pdf_baking import PDFBakeService
            pdf_content = PDFBakeService.bake_page_edits(page)
            
            from django.http import HttpResponse
            response = HttpResponse(pdf_content, content_type='application/pdf')
            response['Content-Disposition'] = f'inline; filename="preview_{doc_ref}_p{page_number}.pdf"'
            return response
        except Exception as e:
            return Response({'error': str(e)}, status=400)

    # POST /api/v1/processing/workspace/content/<doc_ref>/<page_number>/sync/
    @action(detail=False, methods=['post'], url_path=r'content/(?P<doc_ref>[^/]+)/(?P<page_number>\d+)/sync')
    def sync_page_state(self, request, doc_ref=None, page_number=None):
        """
        Replaces ALL blocks, tables, and images on the page with the state provided in the request payload.
        Used for restoring full-page snapshot history (Undo/Redo).
        """
        try:
            assignment = self._get_assignment(doc_ref, int(page_number), request.user)
            page = assignment.page
            
            data = request.data
            blocks_data = data.get('blocks', [])
            tables_data = data.get('tables', [])
            images_data = data.get('images', [])

            with transaction.atomic():
                # 1. Delete all existing elements
                Block.objects.filter(page=page).delete()
                PageTable.objects.filter(page=page).delete()
                PageImage.objects.filter(page=page).delete()

                # 2. Recreate Blocks
                for b_data in blocks_data:
                    Block.objects.create(
                        page=page,
                        block_index=b_data.get('block_index', 0),
                        block_id=b_data.get('block_id') or str(uuid.uuid4()),
                        block_type=b_data.get('block_type', 'text'),
                        original_text=b_data.get('original_text', ''),
                        current_text=b_data.get('current_text', ''),
                        is_dirty=b_data.get('is_dirty', False),
                        x=b_data.get('x', 0),
                        y=b_data.get('y', 0),
                        width=b_data.get('width', 0),
                        height=b_data.get('height', 0),
                        bbox=b_data.get('bbox', [0,0,0,0]),
                        font_name=b_data.get('font_name', ''),
                        font_size=b_data.get('font_size', 0),
                        font_weight=b_data.get('font_weight', ''),
                        font_style=b_data.get('font_style', ''),
                        font_color=b_data.get('font_color', ''),
                        # Skip table_id/row_index/col_index for purely geometric layout restores, as tables manage their own cells.
                    )

                # 3. Recreate Tables
                for t_data in tables_data:
                    PageTable.objects.create(
                        page=page,
                        table_ref=t_data.get('table_ref') or f"table_{page.id}_{uuid.uuid4().hex[:6]}",
                        x=t_data.get('x', 0),
                        y=t_data.get('y', 0),
                        width=t_data.get('width', 0),
                        height=t_data.get('height', 0),
                        row_count=t_data.get('row_count', 1),
                        col_count=t_data.get('col_count', 1),
                        table_json=t_data.get('table_json', [[""]]),
                        col_widths=t_data.get('col_widths', [100.0]),
                        row_heights=t_data.get('row_heights', [100.0]),
                        col_aligns=t_data.get('col_aligns', ['left']),
                        row_colors=t_data.get('row_colors', ['#ffffff']),
                        has_borders=t_data.get('has_borders', True),
                        has_header=t_data.get('has_header', False),
                    )

                # 4. Recreate Images
                for i_data in images_data:
                    img = PageImage.objects.create(
                        page=page,
                        image_ref=i_data.get('image_ref') or f"IMG_{uuid.uuid4().hex[:6]}",
                        x=i_data.get('x', 0),
                        y=i_data.get('y', 0),
                        width=i_data.get('width', 0),
                        height=i_data.get('height', 0),
                    )
                    # Note: We cannot easily restore the actual binary image_file upload via simple JSON sync if the file was deleted.
                    # However, since we simply deleted the row, the file might theoretically be orphaned or we'd just leave the box blank if unlinked.
                    # For a perfect timeline restore, we'd need to copy the file references if they existed.
                    # Since images are generally manual uploads in this app (which are static paths), we can just restore the URL reference if needed.
                    
            return Response({'status': 'synced'})
        except Exception as e:
            logger.error(f"[WorkspaceViewSet] Error in sync_page_state: {str(e)}", exc_info=True)
            return Response({'error': str(e)}, status=400)

    # POST /api/v1/processing/workspace/content/<doc_ref>/<page_number>/start/
    @action(detail=False, methods=['post'], url_path=r'content/(?P<doc_ref>[^/.]+)/(?P<page_number>\d+)/start')
    def start_processing(self, request, doc_ref=None, page_number=None):
        """Triggers the timestamp start for all pages in the user's block"""
        try:
            target_assignment = self._get_assignment(doc_ref, page_number, request.user)
            
            # Start ALL active assignments for this user on this document
            assignments = PageAssignment.objects.filter(
                document=target_assignment.document,
                resource__user=request.user,
                status=PageAssignmentStatus.ASSIGNED
            )
            
            now = timezone.now()
            with transaction.atomic():
                for a in assignments:
                    a.status = PageAssignmentStatus.IN_PROGRESS
                    if not a.processing_start_at:
                        a.processing_start_at = now
                    a.save()
                    
                    # Also update the Page model for consistency
                    page = a.page
                    if not page.processing_started_at:
                        page.processing_started_at = now
                        page.processing_start_date = now.date()
                        page.processing_start_time = now.time()
                        page.save(update_fields=['processing_started_at', 'processing_start_date', 'processing_start_time'])
            
            return Response({'status': 'started', 'processing_start_at': target_assignment.processing_start_at})
        except Exception as e:
            return Response({'error': True, 'code': 'ERROR', 'message': str(e)}, status=400)

    # POST /api/v1/processing/workspace/content/<doc_ref>/<page_number>/submit/
    @action(detail=False, methods=['post'], url_path=r'content/(?P<doc_ref>[^/.]+)/(?P<page_number>\d+)/submit')
    def submit_processing(self, request, doc_ref=None, page_number=None):
        """Finalizes the entire block assigned to this user for this document"""
        try:
            target_assignment = self._get_assignment(doc_ref, page_number, request.user)
            
            # 1. Fetch all assignments in this user's block for this doc
            assignments = PageAssignment.objects.filter(
                document=target_assignment.document,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            )
            
            # 2. Trigger completion pipeline for each
            with transaction.atomic():
                for a in assignments:
                    ProcessingService.complete_assignment(a.id, request.user)
                    
            return Response({'status': 'submitted', 'block_completed': assignments.count()})
        except PermissionError as e:
            return Response({'error': True, 'code': 'FORBIDDEN', 'message': str(e)}, status=403)
        except PageAssignment.DoesNotExist:
            return Response({'error': True, 'code': 'NOT_FOUND', 'message': 'Target assignment not found'}, status=404)
        except Exception as e:
            logger.error(f"Submit API Error: {e}", exc_info=True)
            return Response({'error': True, 'code': 'ERROR', 'message': str(e)}, status=400)


class ProcessingAdminViewSet(viewsets.ViewSet):
    """
    Section 11 APIs for the Admin panel.
    """
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    @staticmethod
    def _format_duration(value):
        if not value:
            return None
        total_seconds = int(value.total_seconds())
        minutes, seconds = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m {seconds}s"
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    @staticmethod
    def _matching_block_assignments(primary: PageAssignment):
        base_qs = PageAssignment.objects.filter(
            document=primary.document,
            resource=primary.resource,
            status=primary.status,
        ).select_related("page", "resource__user", "reassigned_from__resource__user")

        source_resource_id = (
            primary.reassigned_from.resource_id
            if primary.reassigned_from and primary.reassigned_from.resource_id
            else None
        )
        if source_resource_id is None:
            base_qs = base_qs.filter(reassigned_from__resource__isnull=True)
        else:
            base_qs = base_qs.filter(reassigned_from__resource_id=source_resource_id)

        ordered = list(base_qs.order_by("page__page_number", "assigned_at", "id"))
        if not ordered:
            return [primary]

        target_id = primary.id
        block = []
        current = []
        prev_page = None

        for assignment in ordered:
            page_number = assignment.page.page_number
            if not current:
                current = [assignment]
            elif prev_page is not None and page_number == prev_page + 1:
                current.append(assignment)
            else:
                if any(x.id == target_id for x in current):
                    block = current
                    break
                current = [assignment]
            prev_page = page_number

        if not block and current and any(x.id == target_id for x in current):
            block = current

        return block or [primary]

    @action(detail=False, methods=["get"], url_path=r"assignments/(?P<assignment_id>\d+)/artifacts")
    def assignment_artifacts(self, request, assignment_id=None):
        """
        Return uploaded/submitted files and change metrics for the assignment block.
        Used by Admin "View" in Assignment Trace.
        """
        primary = get_object_or_404(
            PageAssignment.objects.select_related(
                "document", "resource__user", "page", "reassigned_from__resource__user"
            ),
            id=assignment_id,
        )

        block_assignments = self._matching_block_assignments(primary)
        assignment_ids = [a.id for a in block_assignments]

        submissions = SubmittedPage.objects.filter(assignment_id__in=assignment_ids).select_related(
            "submitted_by", "reviewed_by", "assignment"
        )
        submission_by_assignment = {s.assignment_id: s for s in submissions}

        # Grouping Logic: Consolidate pages by Bundle
        grouped_payload = {} # bundle_id or "p_{page_number}" -> payload
        bundle_map = {} # bundle_id -> list of page entries
        
        uploaded_count = 0
        total_words = 0
        total_blocks_edited = 0
        total_blocks = 0

        for assignment in block_assignments:
            submission = submission_by_assignment.get(assignment.id)
            file_url = None
            if submission and submission.output_page_file:
                try:
                    file_url = request.build_absolute_uri(submission.output_page_file.url)
                except Exception:
                    file_url = submission.output_page_file.url

            edited_blocks = 0
            if submission:
                edited_blocks = submission.blocks_edited or len(submission.edited_blocks_json or [])
            elif assignment.edited_blocks_json:
                edited_blocks = len(assignment.edited_blocks_json or [])

            blocks_total = submission.blocks_total if submission else 0
            words_processed = submission.words_processed if submission else 0

            if file_url:
                uploaded_count += 1
            total_words += words_processed
            total_blocks_edited += edited_blocks
            total_blocks += blocks_total or 0

            page_data = {
                "assignment_id": assignment.id,
                "page_number": assignment.page.page_number,
                "assignment_status": assignment.status,
                "assignment_status_display": assignment.get_status_display(),
                "assigned_at": assignment.assigned_at,
                "started_at": assignment.processing_start_at,
                "submitted_at": assignment.submitted_at,
                "processing_duration": self._format_duration(assignment.processing_duration),
                "submission_id": submission.id if submission else None,
                "submission_status": submission.review_status if submission else None,
                "submission_status_display": (
                    submission.get_review_status_display() if submission else None
                ),
                "submission_submitted_at": submission.submitted_at if submission else None,
                "submitted_by": (
                    submission.submitted_by.username
                    if submission and submission.submitted_by
                    else (
                        assignment.resource.user.username
                        if assignment.resource and assignment.resource.user
                        else None
                    )
                ),
                "reviewed_at": submission.reviewed_at if submission else None,
                "reviewed_by": (
                    submission.reviewed_by.username
                    if submission and submission.reviewed_by
                    else None
                ),
                "review_notes": submission.review_notes if submission else "",
                "resource_notes": (
                    submission.resource_notes
                    if submission and submission.resource_notes
                    else assignment.resource_notes
                ),
                "words_processed": words_processed,
                "blocks_edited": edited_blocks,
                "blocks_total": blocks_total,
                "artifact_file_url": file_url,
                "artifact_filename": (
                    submission.output_page_file.name.split("/")[-1]
                    if submission and submission.output_page_file
                    else None
                ),
                "workspace_url": f"/workspace/{assignment.document.doc_ref}/{assignment.page.page_number}/?block_view=true",
                "bundle_id": str(submission.bundle_id) if submission and submission.bundle_id else None
            }

            bid = page_data["bundle_id"]
            if bid:
                if bid not in bundle_map:
                    bundle_map[bid] = []
                bundle_map[bid].append(page_data)
            else:
                # Standalone page
                grouped_payload[f"p_{page_data['page_number']}"] = page_data

        # Merge bundle items
        for bid, pages in bundle_map.items():
            pages.sort(key=lambda x: x["page_number"])
            first = pages[0]
            last = pages[-1]
            
            # Aggregate metrics
            agg_words = sum(p["words_processed"] for p in pages)
            agg_edited = sum(p["blocks_edited"] for p in pages)
            agg_total = sum(p["blocks_total"] for p in pages)
            
            # Determine overall status (if any page is still pending, bundle is pending)
            # Actually, in strict flow, they usually all have the same status
            
            bundle_payload = {
                "is_bundle": True,
                "bundle_id": bid,
                "page_number": first["page_number"], # For sorting
                "display_label": f"{first['page_number']}-{last['page_number']}" if first != last else str(first['page_number']),
                "assignment_id": first["assignment_id"],
                "assignment_status": first["assignment_status"],
                "assignment_status_display": first["assignment_status_display"],
                "assigned_at": first["assigned_at"],
                "submitted_at": first["submitted_at"],
                "submission_status": first["submission_status"],
                "submission_status_display": first["submission_status_display"],
                "submission_submitted_at": first["submission_submitted_at"],
                "submitted_by": first["submitted_by"],
                "reviewed_at": first["reviewed_at"],
                "reviewed_by": first["reviewed_by"],
                "artifact_file_url": first["artifact_file_url"],
                "workspace_url": first["workspace_url"],
                "words_processed": agg_words,
                "blocks_edited": agg_edited,
                "blocks_total": agg_total,
                "resource_notes": first["resource_notes"],
                "pages_count": len(pages)
            }
            grouped_payload[bid] = bundle_payload

        doc = primary.document
        pages_payload = sorted(grouped_payload.values(), key=lambda x: x["page_number"])

        return Response(
            {
                "assignment_id": primary.id,
                "document_id": str(doc.id),
                "document_ref": doc.doc_ref,
                "document_name": doc.name or doc.title,
                "resource": (
                    primary.resource.user.username
                    if primary.resource and primary.resource.user
                    else None
                ),
                "assignment_status": primary.status,
                "assignment_status_display": primary.get_status_display(),
                "range_start": pages_payload[0]["page_number"] if pages_payload else None,
                "range_end": pages_payload[-1]["page_number"] if pages_payload else None,
                "total_pages": len(pages_payload),
                "uploaded_pages": uploaded_count,
                "total_words_processed": total_words,
                "total_blocks_edited": total_blocks_edited,
                "total_blocks": total_blocks,
                "pages": pages_payload,
            }
        )


    # POST /api/admin/pages/<doc_ref>/<page_number>/review/
    @action(detail=False, methods=['post'], url_path=r'pages/(?P<doc_ref>[^/.]+)/(?P<page_number>\d+)/review')
    def review_page(self, request, doc_ref=None, page_number=None):
        action = request.data.get('action') # 'approve' or 'reject'
        
        submission = get_object_or_404(
            SubmittedPage,
            document__doc_ref=doc_ref,
            page_number=page_number
        )
        
        from common.enums import ReviewStatus
        with transaction.atomic():
            if action == 'approve':
                submission.review_status = ReviewStatus.APPROVED
                submission.reviewed_by = request.user
                submission.reviewed_at = timezone.now()
                submission.save() # Signal triggers document merge check
                return Response({'status': 'approved'})
                
            elif action == 'reject':
                submission.review_status = ReviewStatus.REJECTED
                submission.reviewed_by = request.user
                submission.reviewed_at = timezone.now()
                submission.review_notes = request.data.get('notes', '')
                submission.save()
                
                # Create RejectedPage record for this page
                from apps.processing.models import RejectedPage
                reason = request.data.get('reason', 'QUALITY_FAIL')
                rejected = RejectedPage.objects.create(
                    submission=submission,
                    document=submission.document,
                    page=submission.page,
                    page_number=submission.page_number,
                    rejected_by=request.user,
                    original_resource=submission.assignment.resource,
                    rejection_reason=reason,
                    rejection_notes=request.data.get('notes', '')
                )
                # Mark the original resource as excluded for this page
                from apps.accounts.models import ResourceProfile
                try:
                    res_profile = ResourceProfile.objects.get(user=submission.submitted_by)
                    rejected.excluded_resources.add(res_profile)
                except ResourceProfile.DoesNotExist:
                    logger.warning(f"No ResourceProfile found for user {submission.submitted_by.username} during rejection.")
                
                # === Block Rejection: Find all submitted pages from same resource+doc ===
                from apps.processing.models import SubmittedPage
                from common.enums import ReviewStatus as RS
                sibling_submissions = SubmittedPage.objects.filter(
                    document=submission.document,
                    submitted_by=submission.submitted_by,
                    review_status=RS.PENDING_REVIEW
                ).exclude(id=submission.id)
                
                # Reject siblings too
                for sibling in sibling_submissions:
                    sibling.review_status = RS.REJECTED
                    sibling.reviewed_by = request.user
                    sibling.reviewed_at = timezone.now()
                    sibling.review_notes = request.data.get('notes', 'Rejected as part of block')
                    sibling.save()
                    # Reassign sibling
                    AssignmentService.reassign_rejected_assignment(sibling.assignment.id, request.user)
                
                # Reassign primary
                AssignmentService.reassign_rejected_assignment(submission.assignment.id, request.user)
                total_rejected = 1 + sibling_submissions.count()
                return Response({'status': 'rejected_and_reassigned', 'pages_rejected': total_rejected})
                
        return Response({'error': 'Invalid action'}, status=400)

    # POST /api/v1/processing/admin/assignments/<id>/reject/
    @action(detail=False, methods=['post'], url_path=r'assignments/(?P<assignment_id>\d+)/reject')
    def reject_assignment(self, request, assignment_id=None):
        """Admin revokes an active assignment and triggers reassignment for the full block."""
        try:
            from apps.processing.models import PageAssignment
            
            # 1. Get the primary assignment being rejected
            primary = get_object_or_404(PageAssignment, id=assignment_id)
            
            # 2. Determine status group based on primary assignment to isolate blocks
            if primary.status in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]:
                status_group = [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            else:
                status_group = [primary.status]

            # 3. Find ALL other active or submitted assignments in the SAME state group for the same resource + document
            block_assignments = PageAssignment.objects.filter(
                document=primary.document,
                resource=primary.resource,
                status__in=status_group
            ).order_by('page__page_number')
            
            block_ids = list(block_assignments.values_list('id', flat=True))
            logger.info(f"Block rejection: Rejecting {len(block_ids)} assignments for resource={primary.resource.user.username}, doc={primary.document.name}")
            
            # 3. Reassign all pages in the block
            from apps.processing.models import SubmittedPage
            from common.enums import ReviewStatus
            
            # 3. Perform atomic block reassignment
            reassigned_count = 0
            new_resource_name = None
            
            with transaction.atomic():
                # Get all pages in the block
                page_ids = list(block_assignments.values_list('page_id', flat=True))
                
                # a. Cancel old assignments
                for assignment in block_assignments:
                    assignment.status = PageAssignmentStatus.REASSIGNED
                    assignment.processing_end_at = timezone.now()
                    assignment.save(update_fields=['status', 'processing_end_at'])
                    # If SUBMITTED, clean up SubmittedPage
                    if assignment.status == PageAssignmentStatus.SUBMITTED:
                        sp = SubmittedPage.objects.filter(assignment=assignment, review_status=ReviewStatus.PENDING_REVIEW).first()
                        if sp:
                            sp.review_status = ReviewStatus.REJECTED
                            sp.reviewed_by = request.user
                            sp.reviewed_at = timezone.now()
                            sp.save()

                # b. Reset pages to PENDING temporarily for the engine to see them
                Page.objects.filter(id__in=page_ids).update(
                    status=PageStatus.PENDING, 
                    current_assignee=None, 
                    is_locked=False
                )

                # c. Explicitly reassign the WHOLE block at once if possible
                # This ensures the entire range 1,2,3,7,8,9 stays together
                from apps.processing.services.core import AssignmentService
                
                # Get candidate resources excluding the one we just rejected
                # We reuse the logic from reassign_rejected_assignment but for a block
                resources = AssignmentService.get_available_resources().exclude(id=primary.resource.id)
                best_new_res = resources.first()

                if best_new_res:
                    # Record the link for the VERY FIRST page to satisfy the trace link_map
                    # Actually, for the trace to show "Reassigned to...", 
                    # we need the new PageAssignment to have reassigned_from = old_PageAssignment.
                    
                    # Store old assignments by page_id for linking
                    old_map = {a.page_id: a for a in block_assignments}
                    
                    for pid in page_ids:
                        target_page = Page.objects.get(id=pid)
                        prev_asgn = old_map.get(pid)
                        
                        PageAssignment.objects.create(
                            page=target_page,
                            resource=best_new_res,
                            document=primary.document,
                            status=PageAssignmentStatus.ASSIGNED,
                            max_processing_time=600,
                            is_reassigned=True,
                            reassigned_from=prev_asgn,
                            reassignment_count=(prev_asgn.reassignment_count if prev_asgn else 0) + 1
                        )
                        target_page.status = PageStatus.ASSIGNED
                        target_page.current_assignee = best_new_res.user
                        target_page.save(update_fields=['status', 'current_assignee'])
                    
                    best_new_res.refresh_status()
                    new_resource_name = best_new_res.user.username
                    reassigned_count = len(page_ids)
                else:
                    # If no single resource can take the block, fall back to general pool
                    reassigned_count = AssignmentService.assign_pages(primary.document.id)
            
            if new_resource_name:
                return Response({
                    'status': 'success',
                    'message': f'Full block of {len(page_ids)} page(s) rejected and reassigned to {new_resource_name}.',
                    'new_resource': new_resource_name,
                    'reassigned_count': reassigned_count
                })
            else:
                return Response({
                    'status': 'success',
                    'message': f'Full block rejected. {reassigned_count} page(s) reassigned to available pool.',
                    'reassigned_count': reassigned_count
                })
                
        except PageAssignment.DoesNotExist:
            return Response({'error': 'Assignment not found'}, status=404)
        except Exception as e:
            logger.error(f"Block rejection failed: {e}")
            return Response({'error': str(e)}, status=500)

    @action(detail=False, methods=['post'], url_path=r'assignments/(?P<assignment_id>\d+)/approve')
    def approve_assignment(self, request, assignment_id=None):
        """
        Admin approves a submitted assignment block.
        Marks all SUBMITTED pages in the block as APPROVED.
        """
        primary = get_object_or_404(PageAssignment, id=assignment_id)
        
        # Block Approval: Find all assignments for same resource + document that are SUBMITTED
        block_assignments = PageAssignment.objects.filter(
            document=primary.document,
            resource=primary.resource,
            status=PageAssignmentStatus.SUBMITTED
        )
        
        if not block_assignments.exists():
            return Response({'error': 'No submitted assignments found for this block.'}, status=400)

        from apps.processing.models import SubmittedPage
        from common.enums import ReviewStatus
        
        count = 0
        with transaction.atomic():
            for a in block_assignments:
                # 1. Update Assignment Status
                a.status = PageAssignmentStatus.APPROVED
                a.save()
                
                # 2. Update SubmittedPage status
                sp = SubmittedPage.objects.filter(assignment=a, review_status=ReviewStatus.PENDING_REVIEW).first()
                if sp:
                    sp.review_status = ReviewStatus.APPROVED
                    sp.reviewed_by = request.user
                    sp.reviewed_at = timezone.now()
                    sp.save() # This triggers document merge check via signal
                    count += 1
                
                # 3. Update Page status
                page = a.page
                from common.enums import PageStatus
                page.status = PageStatus.COMPLETED
                page.save()

                # 4. Trigger Bundle Re-computation (if applicable)
                if sp and sp.bundle_id:
                    from apps.desktop_bridge.services import recompute_bundle_status
                    recompute_bundle_status(sp.bundle_id)

        logger.info(f"Block Approval: {count} pages approved for resource {primary.resource.user.username} on doc {primary.document.name}")
        
        return Response({
            'status': 'success',
            'message': f'Approved {count} pages for {primary.resource.user.username}.',
            'approved_count': count
        })

    # POST /api/admin/documents/<doc_ref>/approve/
    @action(detail=False, methods=['post'], url_path=r'documents/(?P<doc_ref>[^/.]+)/approve')
    def approve_document(self, request, doc_ref=None):
        """Manual shortcut for force overriding documents if needed"""
        doc = get_object_or_404(Document, doc_ref=doc_ref)
        
        # Manual trigger of MergeService
        from apps.processing.tasks import merge_document_pages
        merge_document_pages.delay(doc.id, request.user.id)
        return Response({'status': 'triggered', 'message': 'Document merge triggered.'})

    # GET /api/admin/reassignment-log/
    @action(detail=False, methods=['get'], url_path='reassignment-log')
    def get_reassignments(self, request):
        logs = ReassignmentLog.objects.select_related(
            'previous_resource__user', 'new_resource__user', 'reassigned_by'
        ).order_by('-created_at')[:50]
        
        from apps.processing.serializers import ReassignmentLogSerializer
        return Response(ReassignmentLogSerializer(logs, many=True).data)

    # GET /api/v1/processing/admin/dashboard/
    @action(detail=False, methods=['get'], url_path='dashboard')
    def get_dashboard(self, request):
        from apps.accounts.models import User, ResourceProfile
        from apps.documents.models import Document, Page
        from common.enums import DocumentStatus, PageStatus, ResourceStatus
        from django.db.models import Count
        
        # 1. Document Stats
        total_docs = Document.objects.count()
        processing_docs = Document.objects.filter(status__in=[DocumentStatus.ASSIGNED, DocumentStatus.IN_PROGRESS]).count()
        pending_reviews = Document.objects.filter(status=DocumentStatus.REVIEWING).count()
        unassigned_docs = Document.objects.filter(status__in=[DocumentStatus.UPLOADED, DocumentStatus.SPLITTING]).count()
        
        # 2. User Stats
        total_users = User.objects.count()
        active_res = ResourceProfile.objects.filter(status=ResourceStatus.ACTIVE).count()
        busy_res = ResourceProfile.objects.filter(status=ResourceStatus.BUSY).count()
        
        # 3. Page Stats
        assigned_pages = Page.objects.filter(status=PageStatus.ASSIGNED).count()
        unassigned_pages = Page.objects.filter(status=PageStatus.PENDING).count()
        
        return Response({
            'total_docs': total_docs,
            'total_users': total_users,
            'processing_docs': processing_docs,
            'pending_reviews': pending_reviews,
            'assigned_pages_count': assigned_pages,
            'unassigned_pages_count': unassigned_pages,
            'unassigned_docs_count': unassigned_docs,
            'resources': {
                'active': active_res,
                'busy': busy_res,
                'total_online': active_res + busy_res
            }
        })

from django.shortcuts import render
from django.contrib.auth.decorators import login_required

@login_required
def workspace_view(request, doc_ref, page_number):
    """
    Renders the frontend workspace for a specific page assignment.
    Validates that the user has access to this assignment before rendering.
    """
    is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
    
    # 1. Try to find an assignment (Active or Submitted)
    # This view is mainly an entry point; the actual data comes from the API.
    # We just need to check if the user HAS A REASON to be here.
    
    try:
        # Check for assignment for this user (or any if admin)
        assignments = PageAssignment.objects.filter(
            document__doc_ref=doc_ref,
            page__page_number=page_number
        ).select_related('document', 'page', 'resource__user')
        
        if not is_admin:
            assignments = assignments.filter(
                resource__user=request.user,
                status__in=[
                    PageAssignmentStatus.ASSIGNED, 
                    PageAssignmentStatus.IN_PROGRESS, 
                    PageAssignmentStatus.SUBMITTED,
                    PageAssignmentStatus.APPROVED
                ]
            )
            
        if not assignments.exists():
            # If no assignment, check if the page exists for admin view
            if is_admin:
                page = get_object_or_404(Page, document__doc_ref=doc_ref, page_number=page_number)
                context = {
                    'doc_ref': doc_ref,
                    'page_number': page_number,
                    'pdf_url': page.content_file.url if page.content_file else page.document.file.url,
                    'is_readonly': False, # Admins can edit
                    'assignment': None
                }
                return render(request, 'resource/edit_assignment.html', context)
            return render(request, 'error.html', {'message': 'Assignment not found.', 'code': 404}, status=404)
        
        # Take the "most relevant" assignment
        assignment = assignments.latest('assigned_at')
        
        # Determine read-only based on status OR role
        is_readonly = assignment.status not in [PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
        
        # Even if admin, if viewing a submitted page, it's read-only in the editor layer
        
        pdf_url = assignment.document.file.url if assignment.document.file else ''
            
        context = {
            'doc_ref': doc_ref,
            'page_number': page_number,
            'assignment': assignment,
            'pdf_url': pdf_url,
            'is_readonly': is_readonly
        }
        return render(request, 'resource/edit_assignment.html', context)
        
    except Exception as e:
        logger.error(f"Workspace rendering error: {e}")
        return render(request, 'error.html', {'message': str(e), 'code': 500}, status=500)


# ── Section 11: Capacity & Rebalancing (New Spec) ──────────

class ResourceCapacityUpdateView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def patch(self, request, resource_id):
        resource = get_object_or_404(
            ResourceProfile.objects.select_for_update(),
            pk=resource_id
        )
        new_capacity = request.data.get('max_capacity')

        # Validate new capacity value
        if new_capacity is None:
            return Response(
                {'error': True, 'code': 'MISSING_FIELD',
                 'message': 'max_capacity is required'},
                status=400
            )

        try:
            new_capacity = int(new_capacity)
            if new_capacity < 1 or new_capacity > 100:
                raise ValueError()
        except (ValueError, TypeError):
            return Response(
                {'error': True, 'code': 'INVALID_VALUE',
                 'message': 'max_capacity must be an integer between 1 and 100'},
                status=400
            )

        old_capacity = resource.max_capacity

        with transaction.atomic():
            # ── Step 1: Update capacity ───────────────────────
            resource.max_capacity = new_capacity
            resource.save(update_fields=['max_capacity'])

            # ── Step 2: Recompute current load from DB ────────
            current_load   = resource.current_load
            remaining      = resource.remaining_capacity

            # ── Step 3: Refresh status ────────────────────────
            resource.refresh_status()

            # ── Step 4: Rebalance if capacity was reduced ─────
            rebalanced_pages = []
            if new_capacity < old_capacity and current_load > new_capacity:
                rebalanced_pages = rebalance_overloaded_resource(resource)

            # ── Step 5: Broadcast status update via WebSocket ─
            # ── Step 5: Broadcast status update via WebSocket ─
            broadcast_resource_status(resource)

        return Response({
            'success':          True,
            'resource_id':      resource_id,
            'username':         resource.user.username,
            'old_capacity':     old_capacity,
            'new_capacity':     new_capacity,
            'current_load':     current_load,
            'remaining':        remaining,
            'status':           resource.status,
            'rebalanced_pages': len(rebalanced_pages),
            'message':          f'Capacity updated from {old_capacity} '
                              f'to {new_capacity}. '
                              f'Current load: {current_load}. '
                              f'Remaining: {remaining}.',
        })


def rebalance_overloaded_resource(resource):
    """
    When capacity is reduced below current load:
    unassign excess pages (lowest priority first)
    and put them back in the assignment queue.
    """
    from apps.processing.models import PageAssignment
    from common.enums import PageAssignmentStatus, PageStatus
    
    overflow = resource.current_load - resource.max_capacity
    if overflow <= 0:
        return []

    # Get assigned pages ordered by lowest priority (unassign these first)
    excess_assignments = PageAssignment.objects.filter(
        resource=resource,
        status=PageAssignmentStatus.ASSIGNED          # only unassign not-yet-started pages
    ).select_related('page').order_by(
        '-page__complexity_weight'  # unassign heaviest first to fix faster
    )

    unassigned = []
    freed_weight = 0

    with transaction.atomic():
        for assignment in excess_assignments:
            if freed_weight >= overflow:
                break
            assignment.status = PageAssignmentStatus.UNASSIGNED
            assignment.save(update_fields=['status'])
            
            assignment.page.status = PageStatus.PENDING
            assignment.page.save(update_fields=['status'])
            
            freed_weight += assignment.page.complexity_weight or 1
            unassigned.append(assignment.page.id)

    # Re-queue unassigned pages for automatic reassignment
    if unassigned:
        from apps.processing.tasks import assign_pages_task
        assign_pages_task.delay()

    return unassigned


def broadcast_resource_status(resource):
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
                    'current_load':  resource.current_load,
                    'max_capacity':  resource.max_capacity,
                    'remaining':     resource.remaining_capacity,
                }
            }
        )

# ── Section 12: Layout Overhaul API ────────────────────────

from django.shortcuts import get_object_or_404
from rest_framework.permissions import IsAuthenticated
from apps.documents.models import Page, Block, PageTable, BlockEdit
from apps.documents.serializers import BlockSerializer, PageTableSerializer

class PageBlocksAPIView(APIView):
    """
    Returns all blocks and tables for a page, with pre-computed CSS coordinates
    based on the requested container width/height.
    """
    permission_classes = [IsAuthenticated]
    
    def get(self, request, page_id):
        page = get_object_or_404(Page, id=page_id)
        
        # Security Check
        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            # Resource check: must have at least ONE assignment in this document to view anything
            has_doc_access = PageAssignment.objects.filter(
                document=page.document,
                resource__user=request.user
            ).exists()
            
            if not has_doc_access:
                return Response({'error': 'Permission denied. You do not have assignments for this document.'}, status=403)

            # Privacy Check: Hide blocks if assigned to someone ELSE
            is_assigned_to_other = PageAssignment.objects.filter(
                page=page
            ).exclude(resource__user=request.user).exists()
            
            if is_assigned_to_other:
                return Response({
                    'page_id': page_id,
                    'pdf_width': page.pdf_page_width,
                    'pdf_height': page.pdf_page_height,
                    'blocks': [],
                    'tables': [],
                    'is_hidden_context': True
                })

        blocks = page.blocks.all().order_by('y', 'x')
        tables = page.tables.all()
        
        # Get target container dimensions from query params (default to common A4 px)
        css_w = float(request.query_params.get('width', 800))
        css_h = float(request.query_params.get('height', 1131))
        
        blocks_data = []
        for b in blocks:
            data = BlockSerializer(b).data
            # Injects 'css' property: {left, top, width, height, font_size}
            data['css'] = b.get_css_coords(css_w, css_h)
            blocks_data.append(data)
            
        return Response({
            'page_id': page_id,
            'pdf_width': page.pdf_page_width,
            'pdf_height': page.pdf_page_height,
            'blocks': blocks_data,
            'tables': PageTableSerializer(tables, many=True).data
        })

    def post(self, request, page_id):
        """Manually draw a new area/block."""
        page = get_object_or_404(Page, id=page_id)
        
        # Security Check
        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            has_doc_access = PageAssignment.objects.filter(
                page=page,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            ).exists()
            if not has_doc_access:
                return Response({'error': 'Permission denied.'}, status=403)

        b_type = request.data.get('type', 'text')
        x = float(request.data.get('x', 0))
        y = float(request.data.get('y', 0))
        w = float(request.data.get('width', 50))
        h = float(request.data.get('height', 20))

        if b_type == 'table':
            from apps.documents.models import PageTable
            tbl_ref = f"T_NEW_{uuid.uuid4().hex[:6].upper()}"
            tbl = PageTable.objects.create(
                page=page, table_ref=tbl_ref,
                x=x, y=y, width=w, height=h, row_count=1, col_count=1,
                table_json=[[{"text": "[New Table]", "indent": 0}]]
            )
            return Response({'status': 'table_created', 'id': tbl.id, 'table_ref': tbl_ref})
        
        if b_type == 'image':
            from apps.documents.models import PageImage
            img_ref = f"IMG_NEW_{uuid.uuid4().hex[:6].upper()}"
            img = PageImage.objects.create(
                page=page, image_ref=img_ref,
                x=x, y=y, width=w, height=h
            )
            return Response({'status': 'image_created', 'id': img.id, 'image_ref': img_ref})

        # Default: text block
        blk_id = str(uuid.uuid4())
        blk = Block.objects.create(
            page=page, block_id=blk_id, block_type=b_type,
            x=x, y=y, width=w, height=h,
            bbox=[x, y, x + w, y + h],
            current_text=request.data.get('text', ''),
            is_dirty=True
        )
        return Response({'status': 'block_created', 'id': str(blk.id), 'block_id': blk_id})


class BlockSaveView(APIView):
    """
    Atomic update for a text block.
    """
    permission_classes = [IsAuthenticated]
    
    def patch(self, request, block_id):
        block = get_object_or_404(Block, id=block_id)

        # Security Check: Must be the active assignee
        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            has_active = PageAssignment.objects.filter(
                page=block.page,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            ).exists()
            if not has_active:
                return Response({'error': 'Permission denied.'}, status=403)

        text = request.data.get('text')
        new_type = request.data.get('block_type')
        is_und = request.data.get('is_underlined')
        
        if is_und is not None:
            block.is_underlined = bool(is_und)
            
        # Font Properties
        if 'font_name' in request.data:    block.font_name = request.data['font_name']
        if 'font_size' in request.data:    block.font_size = float(request.data['font_size'])
        if 'font_weight' in request.data:  block.font_weight = request.data['font_weight']
        if 'font_style' in request.data:   block.font_style = request.data['font_style']
        if 'font_color' in request.data:   block.font_color = request.data['font_color']
        if 'text_align' in request.data:   block.text_align = request.data['text_align']
        if 'font_variant' in request.data: block.font_variant = request.data['font_variant']

        # Movement / Resizing support
        if 'x' in request.data:      block.x = float(request.data['x'])
        if 'y' in request.data:      block.y = float(request.data['y'])
        if 'width' in request.data:  block.width = float(request.data['width'])
        if 'height' in request.data: block.height = float(request.data['height'])
        
        # Update bbox if any coord changed
        block.bbox = [block.x, block.y, block.x + block.width, block.y + block.height]

        if text is not None:
            block.current_text = text
            
        if new_type and new_type != block.block_type:
            # Special logic: converting to structural table
            if new_type == 'table':
                # Create a PageTable entry so it appears as a grid in the editor
                from apps.documents.models import PageTable
                # Ensure we don't create multiple tables for the same ref
                table_ref = f"T_{block.page_id}_{block.block_id}"
                if not PageTable.objects.filter(table_ref=table_ref).exists():
                    PageTable.objects.create(
                        page=block.page,
                        table_ref=table_ref,
                        x=block.x, y=block.y, 
                        width=block.width, height=block.height,
                        row_count=1, col_count=1,
                        table_json=[[{"text": block.current_text or "[Table Cell]", "indent": 0}]]
                    )
                block.block_type = 'table'
                block.save()
                return Response({'status': 'converted_to_table'})

            if new_type == 'image':
                # Create a PageImage entry
                from apps.documents.models import PageImage
                if not PageImage.objects.filter(page=block.page, x=block.x, y=block.y).exists():
                    PageImage.objects.create(
                        page=block.page,
                        image_ref=f"IMG_{block.block_id}",
                        x=block.x, y=block.y,
                        width=block.width, height=block.height
                    )
                block.block_type = 'image'
                block.save()
                return Response({'status': 'converted_to_image'})
            
            block.block_type = new_type
            
        block.is_dirty = True
        block.last_edited_by = request.user
        block.last_edited_at = timezone.now()
        block.save()
        
        return Response({'status': 'saved', 'block_id': block_id})


    def delete(self, request, block_id):
        block = get_object_or_404(Block, id=block_id)

        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            has_active = PageAssignment.objects.filter(
                page=block.page,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            ).exists()
            if not has_active:
                return Response({'error': 'Permission denied.'}, status=403)

        block.delete()
        return Response({'status': 'deleted'})


class TableCellSaveView(APIView):
    """
    Update a specific table cell. 
    Finds the block matching table_id, row, and col.
    """
    permission_classes = [IsAuthenticated]
    
    def patch(self, request, table_id):
        row = request.data.get('row')
        col = request.data.get('col')
        text = request.data.get('text', '')
        
        # Formatting Props
        f_weight = request.data.get('font_weight')
        f_style  = request.data.get('font_style')
        f_size   = request.data.get('font_size')
        t_align  = request.data.get('text_align')
        is_und   = request.data.get('is_underlined')
        
        # 1. Update the structural Table (The Source of Truth for the Grid)
        table_obj = get_object_or_404(PageTable, table_ref=table_id)
        
        # Security Check
        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            has_active = PageAssignment.objects.filter(
                page=table_obj.page,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            ).exists()
            if not has_active:
                return Response({'error': 'Permission denied.'}, status=403)
        
        # Update JSON grid
        grid = table_obj.table_json
        if row is not None and col is not None:
            try:
                # Ensure row exists
                while len(grid) <= row:
                    grid.append([])
                # Ensure col exists in row
                while len(grid[row]) <= col:
                    grid[row].append({'text': '', 'indent': 0})
                
                cell_val = grid[row][col]
                if not isinstance(cell_val, dict):
                    cell_val = {'text': str(cell_val), 'indent': 0}
                    grid[row][col] = cell_val
                
                # Update text
                if text is not None:
                    cell_val['text'] = text
                
                # Update formatting
                if f_weight is not None: cell_val['font_weight'] = f_weight
                if f_style is not None:  cell_val['font_style'] = f_style
                if f_size is not None:   cell_val['font_size'] = f_size
                if t_align is not None:  cell_val['text_align'] = t_align
                if is_und is not None:   cell_val['is_underlined'] = is_und
                
                table_obj.table_json = grid
                table_obj.save(update_fields=['table_json'])
            except Exception as e:
                logger.error(f"Failed to update table JSON: {e}")

        # 2. Update the specific block (for consistency/Legacy lookups)
        try:
            block = Block.objects.get(table_id=table_id, row_index=row, col_index=col)
            if text is not None:     block.current_text = text
            if f_weight is not None: block.font_weight = f_weight
            if f_style is not None:  block.font_style = f_style
            if f_size is not None:   block.font_size = float(f_size)
            if t_align is not None:  block.text_align = t_align
            if is_und is not None:   block.is_underlined = is_und
            
            block.is_dirty = True
            block.last_edited_by = request.user
            block.last_edited_at = timezone.now()
            block.save()
        except (Block.DoesNotExist, ValueError, TypeError):
            pass # Structural tables might not have individual child blocks for every cell
        
        return Response({'status': 'saved'})

class PageAnalyzeView(APIView):
    """Explicit endpoint for triggering analysis."""
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, page_id):
        page = get_object_or_404(Page, id=page_id)
        from apps.processing.tasks import extract_page_blocks_task
        extract_page_blocks_task.delay(page.id)
        return Response({'status': 'Analysis triggered', 'page_id': page.id})


class PageRecognizeView(APIView):
    """Explicit endpoint for triggering OCR recognition."""
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, page_id):
        page = get_object_or_404(Page, id=page_id)
        from apps.processing.tasks import process_page_ocr_task
        process_page_ocr_task.delay(page.id)
        return Response({'status': 'Recognition triggered', 'page_id': page.id})


class PageTableSaveView(APIView):
    """
    Saves the entire structure of a PageTable. 
    If the table_ref is a placeholder (e.g. table_0_0), it generates a real one.
    """
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, page_id):
        page = get_object_or_404(Page, id=page_id)
        data = request.data
        table_ref = data.get('table_ref')
        
        # Security Check
        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            has_active = PageAssignment.objects.filter(
                page=page,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            ).exists()
            if not has_active:
                return Response({'error': 'Permission denied.'}, status=403)

        import uuid
        is_new = False
        if not table_ref or table_ref.startswith('table_0_') or table_ref == 'table_0_0':
            table_ref = f"T_SAVED_{uuid.uuid4().hex[:8].upper()}"
            is_new = True

        with transaction.atomic():
            # Update or Create
            table_obj, created = PageTable.objects.update_or_create(
                page=page,
                table_ref=table_ref,
                defaults={
                    'x': float(data.get('x', 0)),
                    'y': float(data.get('y', 0)),
                    'width': float(data.get('width', 0)),
                    'height': float(data.get('height', 0)),
                    'row_count': int(data.get('row_count', 1)),
                    'col_count': int(data.get('col_count', 1)),
                    'table_json': data.get('table_json', []),
                    'col_widths': data.get('col_widths', []),
                    'row_heights': data.get('row_heights', []),
                    'is_manually_edited': True
                }
            )
            
        return Response({
            'status': 'Table saved',
            'table_ref': table_ref,
            'is_new': is_new,
            'id': table_obj.id
        })


class PageTableViewSet(viewsets.ModelViewSet):
    """
    Premium Table Management: Supports coordinate updates and structural edits.
    """
    queryset = PageTable.objects.all()
    serializer_class = PageTableSerializer
    permission_classes = [IsAuthenticated]
    lookup_field = 'table_ref'

    def get_object(self):
        """Ultra-precise lookup: Prioritizes unique Database ID (PK)."""
        lookup_value = (
            self.kwargs.get(self.lookup_url_kwarg) or 
            self.kwargs.get(self.lookup_field) or 
            self.kwargs.get('pk')
        )
        
        # 1. Primary Key Lookup (Numeric) — Absolute Precision
        if str(lookup_value).isdigit():
            try:
                obj = self.get_queryset().get(pk=int(lookup_value))
                logger.info(f"Targeting table by ID: {obj.id} (Ref: {obj.table_ref}) on Page: {obj.page_id}")
                return obj
            except (self.queryset.model.DoesNotExist, ValueError):
                logger.error(f"Table ID {lookup_value} not found in database.")
                pass

        # 2. Contextual Reference Lookup (String) 
        # If the frontend uses a string name, we filter for tables on pages assigned to THIS user 
        # to avoid grabbing the wrong 'table_0_0' from another document.
        if lookup_value:
            from apps.processing.models import PageAssignment
            assigned_page_ids = PageAssignment.objects.filter(
                resource__user=self.request.user
            ).values_list('page_id', flat=True)
            
            objs = self.get_queryset().filter(table_ref=lookup_value)
            
            # Prefer the one on an assigned page
            context_objs = objs.filter(page_id__in=assigned_page_ids)
            if context_objs.exists():
                obj = context_objs.order_by('-created_at').first()
                logger.info(f"Targeting table by Contextual Ref: {obj.table_ref} (ID: {obj.id}) on Assigned Page")
                return obj
            
            if objs.exists():
                obj = objs.order_by('-created_at').first()
                logger.warning(f"Targeting table by Global Ref: {obj.table_ref} (ID: {obj.id}) — NO ASSIGNMENT FOUND")
                return obj

        # Fallback to standard DRF lookup logic
        return super().get_object()

    def destroy(self, request, *args, **kwargs):
        """Dissolve the table safely: Preserve all cell text as independent blocks."""
        logger.info(f"Targeted deletion for table reference: {kwargs.get('table_ref') or 'unknown'}")
        
        try:
            instance = self.get_object()
        except Exception:
            logger.error(f"Table not found for deletion: {kwargs}")
            return Response({'error': 'The requested table could not be found.'}, status=404)

        page = instance.page
        
        # ── Security Check ─────────────────────────────────────
        from common.enums import UserRole, PageAssignmentStatus
        from apps.processing.models import PageAssignment
        
        is_admin = request.user.role == UserRole.ADMIN or request.user.is_superuser or request.user.is_staff
        if not is_admin:
            has_permission = PageAssignment.objects.filter(
                page=page,
                resource__user=request.user,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS, PageAssignmentStatus.SUBMITTED]
            ).exists()
            if not has_permission:
                return Response({'error': 'Permission denied.'}, status=403)
        
        # ── Dissolve Logic ─────────────────────────────────────
        from apps.documents.models import Block
        from django.db import transaction
        
        try:
            with transaction.atomic():
                # Dissolve: Update blocks to standard text (preserve content)
                affected_blocks = Block.objects.filter(page=page, table_id=instance.table_ref)
                
                # Coordinate Fallback (for older, loosely linked tables)
                if not affected_blocks.exists():
                    affected_blocks = Block.objects.filter(
                        page=page,
                        x__gte=instance.x - 2, 
                        x__lte=instance.x + instance.width + 2,
                        y__gte=instance.y - 2, 
                        y__lte=instance.y + instance.height + 2,
                        block_type='table_cell'
                    )
                
                # Update all found cell blocks
                affected_blocks.update(
                    table_id='',
                    block_type='text',
                    row_index=None,
                    col_index=None
                )
                
                # Remove the structural table record
                instance.delete()
                logger.info(f"Table deleted successfully: ID={instance.id}")
                return Response(status=status.HTTP_204_NO_CONTENT)
                
        except Exception as e:
            logger.exception(f"Table deletion failed: {e}")
            return Response({'error': f'A database error occurred: {e}'}, status=500)

    def partial_update(self, request, *args, **kwargs):
        """Coordinate/Size sync — also handles column/row width updates from the editor."""
        instance = self.get_object()
        if 'x' in request.data:      instance.x = float(request.data['x'])
        if 'y' in request.data:      instance.y = float(request.data['y'])
        if 'width' in request.data:  instance.width = float(request.data['width'])
        if 'height' in request.data: instance.height = float(request.data['height'])
        # Accept layout updates from the draggable grid line feature in the workspace
        if 'col_widths' in request.data:
            widths = request.data['col_widths']
            if isinstance(widths, list) and len(widths) > 0:
                # Normalize so widths always sum to 100%
                total = sum(float(w) for w in widths)
                if total > 0:
                    instance.col_widths = [round((float(w) / total) * 100, 2) for w in widths]
                    instance.col_count = len(widths)
        if 'row_heights' in request.data:
            heights = request.data['row_heights']
            if isinstance(heights, list) and len(heights) > 0:
                total = sum(float(h) for h in heights)
                if total > 0:
                    instance.row_heights = [round((float(h) / total) * 100, 2) for h in heights]
                    instance.row_count = len(heights)
        instance.save()
        return Response({'status': 'saved'})

    @action(detail=True, methods=['post'], url_path='rows/add')
    def add_row(self, request, table_ref=None):
        try:
            table_obj = self.get_object()
            grid = list(table_obj.table_json or [])
            index = request.data.get('index', len(grid))
            
            if not isinstance(table_obj.row_heights, list): table_obj.row_heights = []
            
            num_rows = len(table_obj.row_heights)
            
            if num_rows == 0:
                table_obj.row_heights = [100.0]
            elif index is not None and 0 <= index <= num_rows:
                # Splitting Logic: To keep existing rows stable, we split the row 
                # at (or before) the insertion point.
                if index == 0:
                    # Inset at top: Split the first (now second) row
                    target_idx = 0
                elif index == num_rows:
                    # Append at bottom: Split the last row
                    target_idx = num_rows - 1
                else:
                    # Insert in middle: Split the row immediately preceding the insert point
                    target_idx = index - 1
                
                orig_h = table_obj.row_heights[target_idx]
                h1 = round(orig_h / 2, 2)
                h2 = round(orig_h - h1, 2)
                
                table_obj.row_heights[target_idx] = h1
                table_obj.row_heights.insert(index, h2)
            else:
                # Default fallback (equal distribution)
                new_row_pct = 100.0 / (num_rows + 1)
                scale_factor = (100.0 - new_row_pct) / 100.0
                table_obj.row_heights = [round(h * scale_factor, 2) for h in table_obj.row_heights]
                table_obj.row_heights.insert(index if index is not None else num_rows, round(new_row_pct, 2))
                
            # Final normalization to ensure sum is exactly 100.0
            current_sum = sum(table_obj.row_heights)
            if current_sum != 100.0:
                diff = 100.0 - current_sum
                table_obj.row_heights[-1] = round(table_obj.row_heights[-1] + diff, 2)
                
            table_obj.row_count = len(table_obj.row_heights)
            
            # Synchronize the JSON grid structure immediately as a fallback
            grid = list(table_obj.table_json or [])
            num_cols = table_obj.col_count or 1
            new_row = [{"text": "", "indent": 0} for _ in range(num_cols)]
            if index <= len(grid):
                grid.insert(index, new_row)
            else:
                grid.append(new_row)
            table_obj.table_json = grid
            
            logger.info(f"Adding row to table {table_obj.table_ref} at index {index}. New row count: {table_obj.row_count}")
            table_obj.save()
            
            from apps.processing.services.ocr import OCRService
            OCRService.reextract_table_text(table_obj)
            
            return Response({'status': 'row_added', 'row_count': table_obj.row_count})
        except Exception as e:
            logger.error(f"Error adding row to table {table_ref}: {e}", exc_info=True)
            return Response({'error': str(e)}, status=500)

    @action(detail=True, methods=['post'], url_path='rows/delete')
    def delete_row(self, request, table_ref=None):
        try:
            table_obj = self.get_object()
            grid = list(table_obj.table_json or [])
            index = request.data.get('index')
            
            if index is None:
                index = len(grid) - 1
                
            if 0 <= index < len(grid):
                grid.pop(index)
                table_obj.table_json = grid
                table_obj.row_count = len(grid)
                
                if isinstance(table_obj.row_heights, list) and index < len(table_obj.row_heights):
                    removed_h = table_obj.row_heights.pop(index)
                    if table_obj.row_heights:
                        # Redistribute the removed percentage back to others to keep height constant
                        total_remaining = sum(table_obj.row_heights)
                        if total_remaining > 0:
                            table_obj.row_heights = [round((h / total_remaining) * 100.0, 2) for h in table_obj.row_heights]
                            
                            # Final normalization
                            current_sum = sum(table_obj.row_heights)
                            if current_sum != 100.0:
                                diff = 100.0 - current_sum
                                table_obj.row_heights[-1] = round(table_obj.row_heights[-1] + diff, 2)
                    else:
                        table_obj.row_heights = []
                        
                table_obj.save()
                
                from apps.processing.services.ocr import OCRService
                OCRService.reextract_table_text(table_obj)
                
                return Response({'status': 'row_deleted', 'row_count': table_obj.row_count})
            return Response({'error': 'Invalid index'}, status=400)
        except Exception as e:
            logger.error(f"Error deleting row from table {table_ref}: {e}", exc_info=True)
            return Response({'error': str(e)}, status=500)

    @action(detail=True, methods=['post'], url_path='reextract')
    def reextract(self, request, table_ref=None):
        try:
            table_obj = self.get_object()
            from apps.processing.services.ocr import OCRService
            OCRService.reextract_table_text(table_obj)
            return Response({'status': 'reextracted', 'row_count': table_obj.row_count})
        except Exception as e:
            logger.error(f"Error re-extracting table {table_ref}: {e}", exc_info=True)
            return Response({'error': str(e)}, status=500)

    @action(detail=True, methods=['post'], url_path='cols/add')
    def add_column(self, request, table_ref=None):
        try:
            table_obj = self.get_object()
            grid = list(table_obj.table_json or [])
            current_cols = table_obj.col_count or 0
            
            index = request.data.get('index')
            if index is None:
                index = (current_cols // 2) if current_cols > 0 else 0
            
            for row in grid:
                if isinstance(row, list):
                    row.insert(index, {"text": "", "indent": 0})
                
            table_obj.table_json = grid
            table_obj.col_count = (table_obj.col_count or 0) + 1
            
            if not isinstance(table_obj.col_widths, list): table_obj.col_widths = []
            num_cols = len(table_obj.col_widths)
            
            if num_cols == 0:
                table_obj.col_widths = [100.0]
            elif index is not None and 0 <= index <= num_cols:
                # Splitting Logic: Keep existing columns stable by splitting the neighbor
                if index == 0:
                    target_idx = 0
                elif index == num_cols:
                    target_idx = num_cols - 1
                else:
                    target_idx = index - 1
                
                orig_w = table_obj.col_widths[target_idx]
                w1 = round(orig_w / 2, 2)
                w2 = round(orig_w - w1, 2)
                
                table_obj.col_widths[target_idx] = w1
                table_obj.col_widths.insert(index, w2)
            else:
                # Standard fallback
                table_obj.col_widths.append(10.0) # Or something small
                total = sum(table_obj.col_widths)
                table_obj.col_widths = [round((w / total) * 100, 2) for w in table_obj.col_widths]
            
            if isinstance(table_obj.col_aligns, list): 
                if index < len(table_obj.col_aligns):
                    table_obj.col_aligns.insert(index, 'left')
                else:
                    table_obj.col_aligns.append('left')
                
            table_obj.save()
            
            from apps.processing.services.ocr import OCRService
            OCRService.reextract_table_text(table_obj)
            
            return Response({'status': 'col_added', 'col_count': table_obj.col_count})
        except Exception as e:
            logger.error(f"Error adding column to table {table_ref}: {e}", exc_info=True)
            return Response({'error': str(e)}, status=500)

    @action(detail=True, methods=['post'], url_path='cols/delete')
    def delete_column(self, request, table_ref=None):
        try:
            table_obj = self.get_object()
            grid = list(table_obj.table_json or [])
            index = request.data.get('index')
            
            if index is None:
                index = (table_obj.col_count - 1) if table_obj.col_count > 0 else None
                
            if index is not None and 0 <= index < table_obj.col_count:
                for row in grid:
                    if index < len(row): row.pop(index)
                table_obj.table_json = grid
                table_obj.col_count -= 1
                
                if isinstance(table_obj.col_widths, list) and index < len(table_obj.col_widths): 
                    w_to_remove = table_obj.col_widths.pop(index)
                    if len(table_obj.col_widths) > 0:
                        merge_target = index - 1 if index > 0 else 0
                        table_obj.col_widths[merge_target] = round(table_obj.col_widths[merge_target] + w_to_remove, 2)
                        
                if isinstance(table_obj.col_aligns, list) and index < len(table_obj.col_aligns):
                    table_obj.col_aligns.pop(index)
                    
                table_obj.save()
                
                from apps.processing.services.ocr import OCRService
                OCRService.reextract_table_text(table_obj)
                
                return Response({'status': 'col_deleted', 'col_count': table_obj.col_count})
            return Response({'error': 'Invalid index'}, status=400)
        except Exception as e:
            logger.error(f"Error deleting column from table {table_ref}: {e}", exc_info=True)
            return Response({'error': str(e)}, status=500)

class PageImageViewSet(viewsets.ModelViewSet):
    """
    Premium Image Management: Supports coordinate updates and deletion.
    """
    queryset = PageImage.objects.all()
    serializer_class = PageImageSerializer
    permission_classes = [IsAuthenticated]

    def partial_update(self, request, *args, **kwargs):
        instance = self.get_object()
        if 'x' in request.data:      instance.x = float(request.data['x'])
        if 'y' in request.data:      instance.y = float(request.data['y'])
        if 'width' in request.data:  instance.width = float(request.data['width'])
        if 'height' in request.data: instance.height = float(request.data['height'])
        instance.save()
        return Response({'status': 'saved'})

    @action(detail=True, methods=['post'], url_path='upload')
    def upload_image(self, request, pk=None):
        instance = self.get_object()
        file_obj = request.FILES.get('file')
        if not file_obj:
            return Response({'error': 'No file provided'}, status=400)
            
        instance.image_file = file_obj
        instance.save()
        
        # Return the absolute URL to immediately display on the frontend
        image_url = request.build_absolute_uri(instance.image_file.url)
        return Response({'status': 'uploaded', 'image_url': image_url})

# ── Real-time Auto-refresh Views ──────────────────────────────────────────

@csrf_exempt
@api_view(['POST'])
@authentication_classes([CsrfExemptSessionAuthentication, TokenAuthentication])
@permission_classes([permissions.IsAuthenticated])
def heartbeat(request):
    """
    Resource person sends POST every 15 seconds.
    Updates last_seen timestamp and current page.
    Returns their current assignment count.
    """
    try:
        profile = request.user.resource_profile
    except ResourceProfile.DoesNotExist:
        return Response({'error': 'Resource profile not found'}, status=404)

    profile.last_seen = timezone.now()
    profile.last_seen_page = request.data.get('current_page', '')

    # Auto-set ACTIVE when heartbeat received if it was INACTIVE
    if profile.status == ResourceStatus.INACTIVE:
        profile.status = ResourceStatus.ACTIVE
        profile.is_available = True

    profile.save(update_fields=['last_seen', 'last_seen_page', 'status', 'is_available'])

    # Return current assignment info for the resource
    assigned_count = PageAssignment.objects.filter(
        resource=profile,
        status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
    ).count()

    return Response({
        'status':          'online',
        'assigned_pages':  assigned_count,
        'current_load':    profile.get_current_load(),
        'remaining':       profile.get_remaining_capacity(),
        'server_time':     timezone.now().isoformat(),
    })

class ResourceStatusListView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def get(self, request):
        """
        Returns all resources with live online status.
        Admin polls this every 10 seconds.
        """
        resources = ResourceProfile.objects.select_related('user').all()

        data = []
        for r in resources:
            data.append({
                'id':             r.pk,
                'username':       r.user.username,
                'full_name':      r.user.get_full_name(),
                'online_status':  r.online_status,
                'is_online':      r.is_online,
                'status':         r.status,
                'last_seen':      r.last_seen.isoformat() if r.last_seen else None,
                'current_load':   r.get_current_load(),
                'max_capacity':   r.max_capacity,
                'remaining':      r.get_remaining_capacity(),
                'assigned_pages': PageAssignment.objects.filter(
                    resource=r,
                    status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
                ).count(),
            })

        return Response({
            'resources':    data,
            'online_count': sum(1 for r in data if r['is_online']),
            'total':        len(data),
            'polled_at':    timezone.now().isoformat(),
        })

class DocumentListRefreshView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def get(self, request):
        """
        Returns recent documents with pipeline status.
        Admin polls every 5 seconds to catch conversions / splits.
        """
        docs = Document.objects.order_by('-created_at')[:50]

        data = []
        for doc in docs:
            total = doc.total_pages or 0
            assigned = Page.objects.filter(
                document=doc,
                status=PageStatus.ASSIGNED
            ).count() if total > 0 else 0
            submitted = SubmittedPage.objects.filter(
                page__document=doc
            ).count() if total > 0 else 0
            approved = SubmittedPage.objects.filter(
                page__document=doc,
                review_status=ReviewStatus.APPROVED
            ).count() if total > 0 else 0

            # Get assignments grouped by resource for "Blocks"
            from apps.processing.models import PageAssignment
            from django.db.models import Min, Max, Count
            resource_assignments = PageAssignment.objects.filter(document=doc).values(
                'resource__user__username', 'resource__id', 'status'
            ).annotate(
                start_page=Min('page__page_number'),
                end_page=Max('page__page_number'),
                count=Count('id')
            ).order_by('start_page')

            assigned_resources = []
            for ra in resource_assignments:
                # Find a representative assignment for this grouping
                asgn = PageAssignment.objects.filter(
                    document=doc, 
                    resource_id=ra['resource__id'], 
                    status=ra['status']
                ).first()
                
                if asgn:
                    assigned_resources.append({
                        'id': asgn.id,
                        'resource_id': ra['resource__id'],
                        'username': ra['resource__user__username'] or 'Unassigned',
                        'start_page': ra['start_page'],
                        'end_page': ra['end_page'],
                        'status': ra['status'],
                        'status_raw': ra['status'],
                    })

            data.append({
                'id':                 doc.pk,
                'doc_ref':            doc.doc_ref,
                'title':              doc.title or doc.name,
                'pipeline_status':    doc.pipeline_status,
                'conversion_status':  doc.conversion_status,
                'final_file':         doc.final_file.url if doc.final_file else None,
                'total_pages':        total,
                'assigned_pages':     assigned,
                'submitted_pages':    submitted,
                'approved_pages':     approved,
                'progress_pct': round((approved / total * 100) if total > 0 else 0, 1),
                'uploaded_at':  doc.created_at.isoformat(),
                'uploaded_by':  doc.client.username,
                'assigned_resources': assigned_resources,
            })

        return Response({
            'documents': data,
            'polled_at': timezone.now().isoformat(),
        })

class AssignmentQueueView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        """
        Admin:    returns full unassigned page queue
        Resource: returns their own assigned pages
        """
        user = request.user
        is_admin = user.role == UserRole.ADMIN or user.is_superuser or user.is_staff

        if is_admin:
            # Admin view — unassigned queue
            unassigned = Page.objects.filter(
                status=PageStatus.UNASSIGNED,
                is_validated=True,
            ).select_related('document').order_by('document__priority', 'page_number')

            queue_data = [{
                'id':          p.pk,
                'doc_ref':     p.document.doc_ref,
                'doc_title':   p.document.title or p.document.name,
                'page_number': p.page_number,
                'complexity':  p.complexity_weight, # Using weight as proxy for complexity string if needed
                'weight':      p.complexity_weight,
                'priority':    p.document.priority,
            } for p in unassigned]

            return Response({
                'role':          'admin',
                'queue':         queue_data,
                'queue_count':   len(queue_data),
                'polled_at':     timezone.now().isoformat(),
            })
        else:
            # Resource view — their assigned pages
            try:
                profile = user.resource_profile
            except ResourceProfile.DoesNotExist:
                return Response({'assignments': [], 'queue_count': 0})

            assignments = PageAssignment.objects.filter(
                resource=profile,
                status__in=[PageAssignmentStatus.ASSIGNED, PageAssignmentStatus.IN_PROGRESS]
            ).select_related('page', 'page__document').order_by('page__document__priority', 'page__page_number')

            from apps.desktop_bridge.services import get_or_create_assignment_bundle
            
            # Group assignments by document to ensure we create bundles for exactly the assigned pages
            doc_page_map = {}
            for a in assignments:
                doc_id = a.page.document_id
                if doc_id not in doc_page_map:
                    doc_page_map[doc_id] = []
                doc_page_map[doc_id].append(a.page.page_number)

            # Map documents to their specific assignment bundles
            doc_bundle_map = {}
            for doc_id, page_nums in doc_page_map.items():
                first_ass = next(a for a in assignments if a.page.document_id == doc_id)
                doc_bundle_map[doc_id] = get_or_create_assignment_bundle(first_ass.page.document, page_nums)

            data = []
            for a in assignments:
                bundle = doc_bundle_map.get(a.page.document_id)

                data.append({
                    'assignment_id': a.pk,
                    'doc_ref':       a.page.document.doc_ref,
                    'doc_title':     a.page.document.title or a.page.document.name,
                    'page_number':   a.page.page_number,
                    'complexity':    a.page.complexity_weight,
                    'status':        a.status,
                    'max_time':      a.max_processing_time,
                    'started_at':    a.processing_start_at.isoformat() if a.processing_start_at else None,
                    'workspace_url': f'/workspace/{a.page.document.doc_ref}/{a.page.page_number}/',
                    'bundle_id':     str(bundle.id) if bundle else None,
                })

            return Response({
                'role':         'resource',
                'assignments':  data,
                'queue_count':  len(data),
                'current_load': profile.get_current_load(),
                'remaining':    profile.get_remaining_capacity(),
                'polled_at':    timezone.now().isoformat(),
            })

class SubmittedPagesQueueView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def get(self, request):
        """
        Returns submitted pages pending admin review.
        Admin polls every 5 seconds.
        """
        pending = SubmittedPage.objects.filter(
            review_status=ReviewStatus.PENDING_REVIEW
        ).select_related('page', 'page__document', 'submitted_by').order_by('submitted_at')

        data = [{
            'id':            s.pk,
            'doc_ref':       s.page.document.doc_ref,
            'doc_title':     s.page.document.title or s.page.document.name,
            'page_number':   s.page.page_number,
            'submitted_by':  s.submitted_by.username,
            'submitted_at':  s.submitted_at.isoformat(),
            'review_url':    f'/admin/review/{s.page.document.doc_ref}/{s.page.page_number}/',
        } for s in pending]

        return Response({
            'pending_review': data,
            'pending_count':  len(data),
            'polled_at':      timezone.now().isoformat(),
        })

class AdminDashboardSummaryView(APIView):
    permission_classes = [permissions.IsAuthenticated, permissions.IsAdminUser]

    def get(self, request):
        """
        Single endpoint that returns everything the admin dashboard
        needs. Poll this every 5 seconds instead of 5 separate calls.
        """
        from apps.accounts.models import ResourceProfile
        from apps.documents.models import Document, Page
        from apps.processing.models import PageAssignment, SubmittedPage
        from common.enums import DocumentStatus, PageStatus, PageAssignmentStatus, ReviewStatus, PipelineStatus
        
        # 1. Resource status counts (Resource Personnel only)
        all_resources = ResourceProfile.objects.all()
        resources_summary = {
            'online':  sum(1 for r in all_resources if r.online_status == 'online'),
            'away':    sum(1 for r in all_resources if r.online_status == 'away'),
            'offline': sum(1 for r in all_resources if r.online_status == 'offline'),
            'total':   all_resources.count(),
        }

        # 2. Document pipeline counts
        pipeline_counts = {}
        for status_value, status_label in PipelineStatus.choices:
            pipeline_counts[status_value] = Document.objects.filter(pipeline_status=status_value).count()
        
        # Specific active pipeline count (Documents Assigned or In Progress)
        pipeline_counts['processing_docs'] = Document.objects.filter(
            status__in=[DocumentStatus.ASSIGNED, DocumentStatus.IN_PROGRESS]
        ).count()

        # 3. Queue stats
        queue_stats = {
            'unassigned_pages': Page.objects.filter(
                status__in=[PageStatus.PENDING, PageStatus.UNASSIGNED]
            ).count(),
            'in_progress_pages': PageAssignment.objects.filter(
                status=PageAssignmentStatus.IN_PROGRESS
            ).count(),
            'assigned_pages': Page.objects.filter(status=PageStatus.ASSIGNED).count(),
            'pending_review': SubmittedPage.objects.filter(
                review_status=ReviewStatus.PENDING_REVIEW
            ).count(),
        }

        # Add total counts for convenience
        pipeline_counts['total'] = Document.objects.count()
        pipeline_counts['unassigned_docs'] = Document.objects.filter(
            status__in=[DocumentStatus.UPLOADED, DocumentStatus.SPLITTING, DocumentStatus.READY]
        ).count()

        return Response({
            'resources':    resources_summary,
            'pipeline':     pipeline_counts,
            'queue':        queue_stats,
            'polled_at':    timezone.now().isoformat(),
        })
