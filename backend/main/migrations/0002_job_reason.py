# Generated by Django 5.1.2 on 2025-02-07 16:57

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="job",
            name="reason",
            field=models.TextField(null=True),
        ),
    ]
