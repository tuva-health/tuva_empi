# Generated by Django 5.1.2 on 2025-03-13 18:24

import django.contrib.postgres.fields
import django.contrib.postgres.functions
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0005_unique_uuid"),
    ]

    operations = [
        migrations.CreateModel(
            name="User",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                (
                    "created",
                    models.DateTimeField(
                        db_default=django.contrib.postgres.functions.TransactionNow()
                    ),
                ),
                (
                    "updated",
                    models.DateTimeField(
                        db_default=django.contrib.postgres.functions.TransactionNow()
                    ),
                ),
                ("idp_user_id", models.TextField(unique=True)),
                (
                    "role",
                    models.TextField(
                        null=True,
                        choices=[
                            ("admin", "Admin"),
                            ("member", "Member"),
                        ],
                    ),
                ),
            ],
        ),
        migrations.AlterField(
            model_name="matchgroupaction",
            name="performed_by",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.DO_NOTHING,
                to="main.user",
            ),
        ),
        migrations.AlterField(
            model_name="personaction",
            name="performed_by",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.DO_NOTHING,
                to="main.user",
            ),
        ),
    ]
