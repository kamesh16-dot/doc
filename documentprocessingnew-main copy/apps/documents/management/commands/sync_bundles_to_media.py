import os
import shutil
from pathlib import Path
from django.core.management.base import BaseCommand
from django.conf import settings
from apps.desktop_bridge.models import AssignmentBundle

class Command(BaseCommand):
    help = 'Syncs edited bundles from the desktop bridge results into the project media folder.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Simulate the sync without copying files',
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Overwrite existing files in the media folder',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        overwrite = options['overwrite']
        
        # We target all bundles that have an uploaded result
        bundles = AssignmentBundle.objects.exclude(result_pdf='').select_related('document')
        
        self.stdout.write(self.style.SUCCESS(f"Found {bundles.count()} bundles with results."))
        
        sync_count = 0
        skip_count = 0
        error_count = 0

        for bundle in bundles:
            try:
                if not bundle.result_pdf:
                    continue

                source_path = Path(bundle.result_pdf.path)
                if not source_path.exists():
                    self.stdout.write(self.style.WARNING(f"Source file missing for Bundle {bundle.id}: {source_path}"))
                    error_count += 1
                    continue

                # Resolve the destination path using the document's created_at date
                # Pattern: documents/originals/%Y/%m/%d/
                doc_date = bundle.document.created_at
                date_str = doc_date.strftime('%Y/%m/%d')
                
                # We'll sync it to the 'originals' folder as the user requested
                dest_dir = Path(settings.MEDIA_ROOT) / "documents" / "originals" / date_str
                
                if not dry_run:
                    dest_dir.mkdir(parents=True, exist_ok=True)

                dest_path = dest_dir / source_path.name

                if dest_path.exists() and not overwrite:
                    self.stdout.write(f"Skipping {source_path.name} (already exists at {dest_path})")
                    skip_count += 1
                    continue

                if dry_run:
                    self.stdout.write(self.style.INFO(f"[DRY-RUN] Would copy {source_path} -> {dest_path}"))
                else:
                    shutil.copy2(source_path, dest_path)
                    self.stdout.write(self.style.SUCCESS(f"Synced: {source_path.name} -> {date_str}/"))
                
                sync_count += 1

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error syncing bundle {bundle.id}: {e}"))
                error_count += 1

        self.stdout.write(self.style.SUCCESS(
            f"\nSync Complete: {sync_count} synced, {skip_count} skipped, {error_count} errors."
        ))
        if dry_run:
            self.stdout.write(self.style.WARNING("This was a DRY RUN. No files were actually copied."))
