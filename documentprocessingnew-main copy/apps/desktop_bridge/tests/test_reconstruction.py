from django.test import TestCase
from django.core.files.uploadedfile import SimpleUploadedFile
from django.utils import timezone
from django.contrib.auth import get_user_model
from apps.documents.models import Document, Page
from apps.desktop_bridge.models import (
    AssignmentBundle, UploadedPDF, PageVersion, MergeManifest
)
from apps.desktop_bridge.services import (
    register_upload, PageVersionResolver, ZeroLossMergeEngine
)
import uuid

User = get_user_model()

class ReconstructionTestCase(TestCase):
    def setUp(self):
        self.client_user = User.objects.create_user(
            username=f"testclient_{uuid.uuid4().hex[:8]}",
            email="test@example.com",
            password="password123",
            role='CLIENT'
        )
        self.doc = Document.objects.create(
            title="Test Doc",
            total_pages=10,
            client=self.client_user
        )
        # Create 10 Page objects
        for i in range(1, 11):
            Page.objects.create(document=self.doc, page_number=i)

    def create_mock_upload(self, page_range, slice_size):
        bundle = AssignmentBundle.objects.create(
            document=self.doc,
            bundle_index=len(self.doc.bundles.all()),
            page_start=page_range[0],
            page_end=page_range[-1],
            page_numbers=list(page_range)
        )
        # Mock PDF file (empty but we just need the record for logic testing)
        # Note: register_upload actually reads the file, so we skip it or mock PdfReader
        return bundle

    def test_resolution_specificity(self):
        """
        Verify that a smaller slice (more specific) wins over a larger slice.
        """
        # 1. Large slice (pages 1-10)
        u1 = UploadedPDF.objects.create(document=self.doc, checksum="hash1")
        b1 = AssignmentBundle.objects.create(document=self.doc, bundle_index=1, page_start=1, page_end=10, page_numbers=list(range(1,11)))
        for i in range(1, 11):
            PageVersion.objects.create(
                document=self.doc,
                page=Page.objects.get(document=self.doc, page_number=i),
                bundle=b1,
                uploaded_pdf=u1,
                page_number=i,
                page_index_in_pdf=i-1,
                slice_size=10
            )

        # 2. Specific slice (pages 2-3)
        u2 = UploadedPDF.objects.create(document=self.doc, checksum="hash2")
        b2 = AssignmentBundle.objects.create(document=self.doc, bundle_index=2, page_start=2, page_end=3, page_numbers=[2,3])
        for i in [2, 3]:
            PageVersion.objects.create(
                document=self.doc,
                page=Page.objects.get(document=self.doc, page_number=i),
                bundle=b2,
                uploaded_pdf=u2,
                page_number=i,
                page_index_in_pdf=i-2,
                slice_size=2
            )

        resolver = PageVersionResolver(self.doc)
        page_map = resolver.resolve()

        # Page 1 should come from U1 (size 10)
        self.assertEqual(page_map[1].uploaded_pdf_id, u1.id)
        # Page 2 should come from U2 (size 2 wins over size 10)
        self.assertEqual(page_map[2].uploaded_pdf_id, u2.id)
        # Page 3 should come from U2
        self.assertEqual(page_map[3].uploaded_pdf_id, u2.id)
        # Page 4 should come from U1
        self.assertEqual(page_map[4].uploaded_pdf_id, u1.id)

    def test_resolution_recency(self):
        """
        Verify that if slice_sizes are equal, the newest wins.
        """
        bundle = AssignmentBundle.objects.create(document=self.doc, bundle_index=1, page_start=1, page_end=1, page_numbers=[1])
        
        u1 = UploadedPDF.objects.create(document=self.doc, checksum="hash1")
        v1 = PageVersion.objects.create(
            document=self.doc,
            page=Page.objects.get(document=self.doc, page_number=1),
            bundle=bundle,
            uploaded_pdf=u1,
            page_number=1,
            page_index_in_pdf=0,
            slice_size=1
        )
        
        import time
        time.sleep(0.1) # Ensure timestamp difference
        
        u2 = UploadedPDF.objects.create(document=self.doc, checksum="hash2")
        v2 = PageVersion.objects.create(
            document=self.doc,
            page=Page.objects.get(document=self.doc, page_number=1),
            bundle=bundle,
            uploaded_pdf=u2,
            page_number=1,
            page_index_in_pdf=0,
            slice_size=1
        )

        resolver = PageVersionResolver(self.doc)
        page_map = resolver.resolve()

        # Page 1 should come from V2 (newest)
        self.assertEqual(page_map[1].id, v2.id)
