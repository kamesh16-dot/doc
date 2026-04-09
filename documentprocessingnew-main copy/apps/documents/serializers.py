
from rest_framework import serializers
from apps.documents.models import Document, Page, Block, BlockEdit, PageTable, PageImage
import logging

logger = logging.getLogger(__name__)

class BlockSerializer(serializers.ModelSerializer):
    class Meta:
        model = Block
        fields = [
            'id', 'block_index', 'block_id', 'block_type',
            'original_text', 'current_text', 'is_dirty',
            'x', 'y', 'width', 'height', 'bbox',
            'font_name', 'font_size', 'font_weight', 'font_style', 'font_color',
            'text_align', 'font_variant',
            'table_id', 'row_index', 'col_index', 'is_underlined'
        ]

class PageTableSerializer(serializers.ModelSerializer):
    class Meta:
        model = PageTable
        fields = [
            'id', 'table_ref', 'x', 'y', 'width', 'height',
            'row_count', 'col_count', 'table_json', 'col_widths', 'row_heights', 'col_aligns', 'row_colors',
            'has_borders', 'has_header'
        ]

class PageImageSerializer(serializers.ModelSerializer):
    image_url = serializers.SerializerMethodField()

    class Meta:
        model = PageImage
        fields = ['id', 'image_ref', 'x', 'y', 'width', 'height', 'image_url']

    def get_image_url(self, obj):
        request = self.context.get('request')
        if obj.image_file and request:
            return request.build_absolute_uri(obj.image_file.url)
        return None

class PageSerializer(serializers.ModelSerializer):
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    complexity_type_display = serializers.CharField(source='get_complexity_type_display', read_only=True)
    validation_status_display = serializers.CharField(source='get_validation_status_display', read_only=True)
    
    blocks = BlockSerializer(many=True, read_only=True)
    tables = PageTableSerializer(many=True, read_only=True)
    images = PageImageSerializer(many=True, read_only=True)

    class Meta:
        model = Page
        fields = (
            'id', 'page_number', 'status', 'status_display', 
            'current_assignee', 'locked_at', 'text_content',
            # Layout data
            'pdf_page_width', 'pdf_page_height', 'blocks_extracted', 'blocks_count', 'has_tables',
            'blocks', 'tables', 'images',
            # Complexity Data
            'complexity_type', 'complexity_type_display', 'complexity_weight',
            'table_count', 'image_count', 'word_count',
            # Processing meta
            'processing_started_at', 'processing_start_date', 'processing_start_time',
            'processing_completed_at', 'processing_end_date', 'processing_end_time',
            'processing_duration_seconds', 'total_time_spent',
            'is_processed', 'is_scanned', 'ocr_provider',
            # Validation Data
            'validation_status', 'validation_status_display', 'validation_errors'
        )
        read_only_fields = (
            'current_assignee', 'locked_at',
            'complexity_type', 'complexity_weight', 'table_count', 'image_count', 'word_count',
            'processing_started_at', 'processing_start_date', 'processing_start_time',
            'processing_completed_at', 'processing_end_date', 'processing_end_time',
            'processing_duration_seconds', 'total_time_spent',
            'is_processed', 'is_scanned',
            'validation_status', 'validation_errors'
        )

class DocumentSerializer(serializers.ModelSerializer):
    status_display = serializers.CharField(source='get_status_display', read_only=True)
    pipeline_status_display = serializers.CharField(source='get_pipeline_status_display', read_only=True)
    pages = PageSerializer(many=True, read_only=True)
    assigned_resources = serializers.SerializerMethodField()
    granular_status = serializers.SerializerMethodField()
    final_word_ready = serializers.SerializerMethodField()
    
    class Meta:
        model = Document
        fields = (
            'id', 'doc_ref', 'title', 'name', 'original_file', 'file', 'converted_pdf', 
            'status', 'status_display', 'pipeline_status', 'pipeline_status_display',
            'priority', 'deadline', 'conversion_error',
            'total_pages', 'created_at', 'updated_at', 'completed_at', 
            'final_file', 'final_word_file', 'final_word_generated_at', 'final_word_error',
            'final_word_manifest', 'final_word_ready', 'pages', 'assigned_resources', 'granular_status',
            'version', 'completion_percentage'
        )
        read_only_fields = (
            'doc_ref', 'status', 'pipeline_status', 'total_pages', 'completed_at',
            'final_file', 'final_word_file', 'final_word_generated_at', 'final_word_error',
            'final_word_manifest', 'final_word_ready', 'version', 'completion_percentage'
        )

    def get_assigned_resources(self, obj):
        try:
            from apps.processing.models import PageAssignment
            from django.utils import timezone
            from django.core.cache import cache
            from datetime import timedelta

            # Ordering by page_number is crucial for sequential grouping
            # (select_related reassigned_from__resource__user avoids N+1)
            assignments = PageAssignment.objects.filter(document=obj)\
                .select_related('resource__user', 'page', 'reassigned_from__resource__user')\
                .order_by('page__page_number')
            
            # Optimized: Pre-map all REASSIGNED assignments to their SUCCESSORS
            reassigned_ids = [a.id for a in assignments if a.status == 'REASSIGNED']
            link_map = {}
            if reassigned_ids:
                successors = PageAssignment.objects.filter(
                    reassigned_from_id__in=reassigned_ids
                ).select_related('resource__user')
                for s in successors:
                    if s.resource and getattr(s.resource, 'user', None):
                        link_map[s.reassigned_from_id] = s.resource.user.username

            results = []
            current_group = None
            
            for a in assignments:
                if not a.resource or not hasattr(a.resource, 'user') or not a.resource.user:
                    continue
                
                res_id = a.resource.user.id
                reassigned_to = link_map.get(a.id)
                source_username = None
                source_res_id = None
                if a.reassigned_from and a.reassigned_from.resource:
                    source_res_id = a.reassigned_from.resource.user.id
                    source_username = a.reassigned_from.resource.user.username
                
                status_display = a.get_status_display()
                if source_username:
                    status_display = f"Assigned (Reassigned from {source_username})"

                # ── Grouping Logic (Sequential / Consecutive) ─────
                # We only group assignments if it's the SAME person, 
                # SAME status context, and the page is CONSECUTIVE.
                is_consecutive = False
                if current_group:
                    prev_page = current_group['pages'][-1]
                    is_consecutive = (a.page.page_number == prev_page + 1)

                is_same_context = (
                    current_group and
                    current_group['res_id'] == res_id and
                    current_group['status_raw'] == a.status and
                    current_group['source_res_id'] == source_res_id and
                    is_consecutive
                )

                if not is_same_context:
                    is_online = cache.get(f"user:{res_id}:online") == "true"
                    current_group = {
                        'id': a.id,
                        'res_id': res_id,                  # Internal key
                        'source_res_id': source_res_id,    # Internal key
                        'username': a.resource.user.username,
                        'pages': [a.page.page_number],
                        'status': status_display,
                        'status_raw': a.status,
                        'viewed_at': a.processing_start_at,
                        'completed_at': a.submitted_at or a.processing_end_at,
                        'is_online': is_online,
                        'assigned_at': a.assigned_at,
                        'max_time': a.max_processing_time if hasattr(a, 'max_processing_time') else 600,
                        'reassigned_to': reassigned_to,
                        'source_username': source_username
                    }
                    results.append(current_group)
                else:
                    current_group['pages'].append(a.page.page_number)
                    if a.processing_start_at and (not current_group['viewed_at'] or a.processing_start_at < current_group['viewed_at']):
                        current_group['viewed_at'] = a.processing_start_at
                    if a.submitted_at and (not current_group['completed_at'] or a.submitted_at > current_group['completed_at']):
                        current_group['completed_at'] = a.submitted_at
                    if reassigned_to:
                        current_group['reassigned_to'] = reassigned_to
            
            # Format and sort by assigned_at to present chronologically in audit trail
            # (Earlier groups first)
            final_results = []
            for group in results:
                pages = sorted(list(set(group['pages'])))
                if not pages: continue
                
                # Format page numbers: e.g., "1-10"
                page_str = f"{pages[0]}-{pages[-1]}" if len(pages) > 1 else str(pages[0])
                
                expires_at = None
                if group['assigned_at']:
                    expires_at = group['assigned_at'] + timedelta(seconds=group['max_time'])

                final_results.append({
                    'id': group['id'],
                    'username': group['username'],
                    'page_number': page_str,
                    'start_page': pages[0],
                    'end_page': pages[-1],
                    'status': group['status'],
                    'status_raw': group['status_raw'],
                    'assigned_at': group['assigned_at'],
                    'viewed_at': group['viewed_at'],
                    'completed_at': group['completed_at'],
                    'expires_at': expires_at,
                    'is_online': group['is_online'],
                    'reassigned_to': group.get('reassigned_to')
                })
            
            return sorted(final_results, key=lambda r: r['assigned_at'] or timezone.now())
        except Exception as e:
            logger.error(f"Error in get_assigned_resources: {e}", exc_info=True)
            return []

    def get_granular_status(self, obj):
        return obj.get_pipeline_status_display()

    def get_final_word_ready(self, obj):
        return bool(getattr(obj, 'final_word_file', None))
        

class DocumentUploadSerializer(serializers.Serializer):
    file = serializers.FileField()

    def validate_file(self, value):
        import os
        from pathlib import Path
        ext = Path(value.name).suffix.lower()
        
        # 1. Extension Check
        allowed = ['.pdf', '.docx', '.doc']
        if ext not in allowed:
            raise serializers.ValidationError(
                f"Unsupported format '{ext}'. Allowed: PDF, DOCX, DOC"
            )

        # 2. Size Check (100MB)
        max_size = 100 * 1024 * 1024
        if value.size > max_size:
            raise serializers.ValidationError(
                f"File size {value.size // (1024*1024)}MB exceeds 100MB limit."
            )
            
        return value
