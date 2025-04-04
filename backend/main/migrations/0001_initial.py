# Generated by Django 5.1.2 on 2024-12-12 21:50

import django.contrib.postgres.functions
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Config",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                (
                    "created",
                    models.DateTimeField(
                        db_default=django.contrib.postgres.functions.TransactionNow()
                    ),
                ),
                ("potential_match_threshold", models.FloatField()),
                ("auto_match_threshold", models.FloatField()),
                ("splink_settings", models.JSONField()),
            ],
        ),
        migrations.CreateModel(
            name="Job",
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
                (
                    "config",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.config"
                    ),
                ),
                ("s3_uri", models.TextField()),
                (
                    "status",
                    models.TextField(
                        choices=[
                            ("new", "New"),
                            ("succeeded", "Succeeded"),
                            ("failed", "Failed"),
                        ]
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="PersonRecordStaging",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created", models.DateTimeField()),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
                    ),
                ),
                ("row_number", models.BigIntegerField(null=True)),
                ("sha256", models.BinaryField(null=True)),
                ("data_source", models.TextField()),
                ("source_person_id", models.TextField()),
                ("first_name", models.TextField()),
                ("last_name", models.TextField()),
                ("sex", models.TextField()),
                ("race", models.TextField()),
                ("birth_date", models.TextField()),
                ("death_date", models.TextField()),
                ("social_security_number", models.TextField()),
                ("address", models.TextField()),
                ("city", models.TextField()),
                ("state", models.TextField()),
                ("zip_code", models.TextField()),
                ("county", models.TextField()),
                ("phone", models.TextField()),
            ],
            options={
                "indexes": [
                    models.Index(
                        condition=models.Q(("sha256__isnull", False)),
                        fields=["sha256"],
                        name="main_personrecordstg_sha256",
                    ),
                    models.Index(
                        condition=models.Q(("sha256__isnull", False)),
                        fields=["row_number"],
                        name="main_personrecordstg_rownum",
                    ),
                ],
            },
        ),
        migrations.CreateModel(
            name="Person",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("uuid", models.UUIDField()),
                ("created", models.DateTimeField()),
                ("updated", models.DateTimeField()),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
                    ),
                ),
                ("version", models.BigIntegerField(db_default=1)),
                ("deleted", models.DateTimeField(null=True)),
                ("record_count", models.BigIntegerField()),
            ],
            options={
                "constraints": [
                    models.CheckConstraint(
                        condition=models.Q(("record_count__gte", 0)),
                        name="record_count_gte_zero",
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="PersonRecord",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created", models.DateTimeField()),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
                    ),
                ),
                (
                    "person",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.person"
                    ),
                ),
                ("person_updated", models.DateTimeField()),
                ("matched_or_reviewed", models.DateTimeField(null=True)),
                ("sha256", models.BinaryField()),
                ("data_source", models.TextField()),
                ("source_person_id", models.TextField()),
                ("first_name", models.TextField()),
                ("last_name", models.TextField()),
                ("sex", models.TextField()),
                ("race", models.TextField()),
                ("birth_date", models.TextField()),
                ("death_date", models.TextField()),
                ("social_security_number", models.TextField()),
                ("address", models.TextField()),
                ("city", models.TextField()),
                ("state", models.TextField()),
                ("zip_code", models.TextField()),
                ("county", models.TextField()),
                ("phone", models.TextField()),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["sha256"], name="main_person_sha256_2ea0dc_idx"
                    )
                ],
            },
        ),
        migrations.CreateModel(
            name="PersonRecordNote",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created", models.DateTimeField()),
                ("updated", models.DateTimeField()),
                (
                    "person_record",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.personrecord",
                    ),
                ),
                ("note", models.TextField()),
                ("author", models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name="MatchGroup",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("uuid", models.UUIDField()),
                ("created", models.DateTimeField()),
                ("updated", models.DateTimeField()),
                ("deleted", models.DateTimeField(null=True)),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
                    ),
                ),
                (
                    "version",
                    models.BigIntegerField(db_default=1),
                ),
                ("matched", models.DateTimeField(null=True)),
            ],
            options={
                "indexes": [
                    models.Index(fields=["uuid"], name="main_matchg_uuid_f24b85_idx")
                ],
            },
        ),
        migrations.CreateModel(
            name="MatchEvent",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created", models.DateTimeField()),
                (
                    "type",
                    models.TextField(
                        choices=[
                            ("new-ids", "New Ids"),
                            ("auto-matches", "Auto Matches"),
                            ("manual-match", "Manual Match"),
                            ("person-split", "Person Split"),
                        ]
                    ),
                ),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="PersonAction",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                (
                    "match_event",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.matchevent",
                    ),
                ),
                (
                    "match_group",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.matchgroup",
                    ),
                ),
                (
                    "person",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.person"
                    ),
                ),
                (
                    "person_record",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.personrecord",
                    ),
                ),
                (
                    "type",
                    models.TextField(
                        choices=[
                            ("add-record", "Add Record"),
                            ("remove-record", "Remove Record"),
                            ("review", "Review"),
                        ]
                    ),
                ),
                ("performed_by", models.TextField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name="SplinkResult",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("created", models.DateTimeField()),
                (
                    "job",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
                    ),
                ),
                (
                    "match_group",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.matchgroup",
                    ),
                ),
                ("match_group_updated", models.DateTimeField()),
                ("match_weight", models.FloatField()),
                ("match_probability", models.FloatField()),
                (
                    "person_record_l",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        related_name="splinkresult_l_set",
                        to="main.personrecord",
                    ),
                ),
                (
                    "person_record_r",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        related_name="splinkresult_r_set",
                        to="main.personrecord",
                    ),
                ),
                ("data", models.JSONField()),
            ],
        ),
        migrations.CreateModel(
            name="MatchGroupAction",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                (
                    "match_event",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.matchevent",
                    ),
                ),
                (
                    "match_group",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.matchgroup",
                    ),
                ),
                (
                    "splink_result",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="main.splinkresult",
                    ),
                ),
                (
                    "type",
                    models.TextField(
                        choices=[
                            ("add-result", "Add Result"),
                            ("remove-result", "Remove Result"),
                            ("update-person", "Update Person"),
                            ("match", "Match"),
                        ]
                    ),
                ),
                ("performed_by", models.TextField(null=True)),
            ],
        ),
        migrations.RunSQL(
            "create extension if not exists pgcrypto", migrations.RunSQL.noop
        ),
    ]
