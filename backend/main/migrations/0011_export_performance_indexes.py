from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0010_job_job_type"),
    ]

    operations = [
        # Composite index for MatchGroup export filtering
        # This index optimizes the most common export query pattern
        migrations.RunSQL(
            """
            CREATE INDEX idx_matchgroup_export_status
            ON main_matchgroup (matched, deleted, job_id)
            WHERE matched IS NULL AND deleted IS NULL;
            """,
            "DROP INDEX IF EXISTS idx_matchgroup_export_status;",
        ),
        # Index for SplinkResult joins in export queries
        # Optimizes the complex joins between MatchGroup and PersonRecord
        migrations.RunSQL(
            """
            CREATE INDEX idx_splinkresult_export_join
            ON main_splinkresult (match_group_id, person_record_l_id, person_record_r_id);
            """,
            "DROP INDEX IF EXISTS idx_splinkresult_export_join;",
        ),
        # Composite index for PersonRecord filtering in exports
        # Optimizes filtering by person_id and data_source combinations
        migrations.RunSQL(
            """
            CREATE INDEX idx_personrecord_export_filter
            ON main_personrecord (person_id, data_source, first_name, last_name);
            """,
            "DROP INDEX IF EXISTS idx_personrecord_export_filter;",
        ),
        # Index for Person table joins in export queries
        # Optimizes joins between PersonRecord and Person tables
        migrations.RunSQL(
            """
            CREATE INDEX idx_person_export
            ON main_person (id, uuid);
            """,
            "DROP INDEX IF EXISTS idx_person_export;",
        ),
        # Additional index for MatchGroup job filtering
        # Optimizes filtering by job_id in export queries
        migrations.RunSQL(
            """
            CREATE INDEX idx_matchgroup_job_export
            ON main_matchgroup (job_id, matched, deleted);
            """,
            "DROP INDEX IF EXISTS idx_matchgroup_job_export;",
        ),
        # Index for SplinkResult match_group_id lookups
        # Optimizes the primary join in export queries
        migrations.RunSQL(
            """
            CREATE INDEX idx_splinkresult_matchgroup
            ON main_splinkresult (match_group_id);
            """,
            "DROP INDEX IF EXISTS idx_splinkresult_matchgroup;",
        ),
    ]
