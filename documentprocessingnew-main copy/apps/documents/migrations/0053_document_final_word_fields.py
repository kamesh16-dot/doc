from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("documents", "0052_block_is_manually_edited"),
    ]

    operations = [
        migrations.AddField(
            model_name="document",
            name="final_word_error",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="document",
            name="final_word_file",
            field=models.FileField(blank=True, null=True, upload_to="documents/finals_word/%Y/%m/%d/"),
        ),
        migrations.AddField(
            model_name="document",
            name="final_word_generated_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="document",
            name="final_word_manifest",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
