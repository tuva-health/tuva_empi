# Generated by Django 5.1.2 on 2025-06-17 23:11

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0008_job_source_uri"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.RemoveField(
                    model_name="job",
                    name="s3_uri",
                ),
            ],
            database_operations=[],
        ),
    ]
