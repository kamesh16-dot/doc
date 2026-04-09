import uuid
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth import get_user_model
from apps.documents.models import Document, Page
from apps.processing.models import PageAssignment
from apps.accounts.models import ResourceProfile
from apps.desktop_bridge.models import DesktopDevice, AssignmentBundle
from common.enums import PageAssignmentStatus, ResourceStatus

User = get_user_model()

class BundleClaimingTest(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="testuser", password="password", role="RESOURCE")
        self.client_user = User.objects.create_user(username="testclient", password="password", role="CLIENT")
        self.client = Client()
        self.client.login(username="testuser", password="password")
        
        # Setup device
        self.device = DesktopDevice.objects.create(
            user=self.user,
            device_name="Test Device",
            machine_id="test-machine-123"
        )
        
        # Setup document and pages
        self.doc = Document.objects.create(client=self.client_user, title="Test Doc", total_pages=10)
        self.page = Page.objects.create(document=self.doc, page_number=1)
        
        # Setup resource profile and assignment
        self.resource = self.user.resource_profile
        self.resource.status = ResourceStatus.ACTIVE
        self.resource.save()

        self.assignment = PageAssignment.objects.create(
            document=self.doc,
            page=self.page,
            resource=self.resource,
            status=PageAssignmentStatus.ASSIGNED
        )
        
        # Setup bundle
        self.bundle = AssignmentBundle.objects.create(
            document=self.doc,
            bundle_index=0,
            page_start=1,
            page_end=1,
            status=AssignmentBundle.Status.READY
        )

    def test_claim_bundle_success(self):
        url = reverse("desktop-acquire-bundle", kwargs={
            "device_id": self.device.id,
            "bundle_id": self.bundle.id
        })
        response = self.client.post(url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        
        # Verify lease
        self.bundle.refresh_from_db()
        self.assertEqual(self.bundle.status, AssignmentBundle.Status.LEASED)
        self.assertEqual(self.bundle.leased_to, self.device)

    def test_claim_bundle_no_assignment(self):
        # Create another doc with no assignment
        other_doc = Document.objects.create(client=self.client_user, title="Other Doc")
        other_bundle = AssignmentBundle.objects.create(
            document=other_doc,
            bundle_index=0,
            page_start=1,
            page_end=1
        )
        
        url = reverse("desktop-acquire-bundle", kwargs={
            "device_id": self.device.id,
            "bundle_id": other_bundle.id
        })
        response = self.client.post(url)
        self.assertEqual(response.status_code, 403)
        self.assertIn("no active assignments", response.json()["detail"])

    def test_claim_bundle_leased_to_other(self):
        # Lease to someone else
        other_user = User.objects.create_user(username="otheruser", password="password")
        other_device = DesktopDevice.objects.create(user=other_user, device_name="Other Device")
        self.bundle.status = AssignmentBundle.Status.LEASED
        self.bundle.leased_to = other_device
        self.bundle.save()
        
        url = reverse("desktop-acquire-bundle", kwargs={
            "device_id": self.device.id,
            "bundle_id": self.bundle.id
        })
        response = self.client.post(url)
        self.assertEqual(response.status_code, 409)
        self.assertIn("leased to another device", response.json()["detail"])

    def test_claim_bundle_expired_reclaim(self):
        # Expired lease from someone else
        other_device = DesktopDevice.objects.create(user=self.user, device_name="My Other Device")
        self.bundle.status = AssignmentBundle.Status.EXPIRED
        self.bundle.leased_to = other_device
        self.bundle.save()
        
        url = reverse("desktop-acquire-bundle", kwargs={
            "device_id": self.device.id,
            "bundle_id": self.bundle.id
        })
        response = self.client.post(url)
        self.assertEqual(response.status_code, 200)
        
        self.bundle.refresh_from_db()
        self.assertEqual(self.bundle.leased_to, self.device)
        self.assertEqual(self.bundle.status, AssignmentBundle.Status.LEASED)

    def test_claim_bundle_admin_bypass(self):
        # Create an Admin user with NO assignment
        admin_user = User.objects.create_user(username="adminuser", password="password", role="ADMIN")
        admin_device = DesktopDevice.objects.create(user=admin_user, device_name="Admin Device")
        
        # Doc with no assignment for admin
        doc = Document.objects.create(client=self.client_user, title="Admin Doc")
        bundle = AssignmentBundle.objects.create(document=doc, bundle_index=0, page_start=1, page_end=1)
        
        self.client.login(username="adminuser", password="password")
        url = reverse("desktop-acquire-bundle", kwargs={
            "device_id": admin_device.id,
            "bundle_id": bundle.id
        })
        response = self.client.post(url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        
        bundle.refresh_from_db()
        self.assertEqual(bundle.leased_to, admin_device)
