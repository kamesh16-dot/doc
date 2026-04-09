import logging
import fitz  # PyMuPDF
from apps.documents.models import Page
from common.enums import ComplexityType

logger = logging.getLogger(__name__)

class ComplexityResult:
    def __init__(self, complexity, weight, table_count, image_count, word_count, block_density=0, image_density=0):
        self.complexity = complexity
        self.weight = weight
        self.table_count = table_count
        self.image_count = image_count
        self.word_count = word_count
        self.block_density = block_density
        self.image_density = image_density

class ComplexityScorer:
    """
    Analyzes page content to assign a difficulty score.
    Used for capacity planning and SLAs (Section 5).
    """

    def score_page(self, page: Page) -> ComplexityResult:
        try:
            doc = fitz.open(page.content_file.path)
            fitz_page = doc[0]

            text_blocks = fitz_page.get_text('blocks')
            
            # find_tables is available in PyMuPDF 1.23+
            table_count = 0
            try:
                tables = fitz_page.find_tables()
                if tables and hasattr(tables, 'tables'):
                    table_count = len(tables.tables)
            except AttributeError:
                pass # Fallback if using older PyMuPDF version

            images = fitz_page.get_images()
            words = fitz_page.get_text('words')

            image_count = len(images) if images else 0
            word_count = len(words) if words else 0
            
            block_density = 0
            image_density = 0
            page_area = fitz_page.rect.width * fitz_page.rect.height
            
            if fitz_page.rect.height > 0 and text_blocks:
                block_density = len(text_blocks) / fitz_page.rect.height
            
            if page_area > 0 and images:
                # Approximate image density based on count vs page area
                image_density = (image_count * 10000) / page_area

            # File size factor
            file_size_kb = 0
            if page.content_file:
                file_size_kb = page.content_file.size / 1024

            doc.close()

            # Force weight to 1 as per new requirements
            complexity = ComplexityType.SIMPLE
            weight = 1

            return ComplexityResult(
                complexity=complexity, 
                weight=weight,
                table_count=table_count, 
                image_count=image_count,
                word_count=word_count,
                block_density=block_density,
                image_density=image_density
            )
            
        except Exception as e:
            logger.error(f"Error scoring page {page.id}: {e}")
            raise  # Let the caller or task handle retry/fallback

    @classmethod
    def apply_score(cls, page: Page):
        scorer = cls()
        result = scorer.score_page(page)
        
        page.complexity_type = result.complexity
        page.complexity_weight = result.weight
        page.complexity_score = result.weight # Keep both in sync
        page.table_count = result.table_count
        page.image_count = result.image_count
        page.word_count = result.word_count
        page.block_density = result.block_density
        page.image_density = result.image_density
        
        from django.utils import timezone
        page.complexity_scored_at = timezone.now()
        page.save(update_fields=[
            'complexity_type', 'complexity_weight', 'complexity_score',
            'table_count', 'image_count', 'word_count', 
            'block_density', 'image_density', 'complexity_scored_at'
        ])
        return result
