from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0011_export_performance_indexes"),
    ]

    operations = [
        # ===== INDEXES FOR GET /potential-matches/{id} ENDPOINT =====
        # Primary index for MatchGroup lookup by ID (most critical for the endpoint)
        # This optimizes the main query: WHERE mg.id = %(match_group_id)s
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_matchgroup_id_primary
            ON main_matchgroup (id)
            WHERE matched IS NULL AND deleted IS NULL;
            """,
            "DROP INDEX IF EXISTS idx_matchgroup_id_primary;",
        ),
        # Composite index for SplinkResult joins in potential match queries
        # Optimizes: sr.match_group_id = mg.id AND (sr.person_record_l_id = pr.id OR sr.person_record_r_id = pr.id)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_splinkresult_potential_match_join
            ON main_splinkresult (match_group_id, person_record_l_id, person_record_r_id);
            """,
            "DROP INDEX IF EXISTS idx_splinkresult_potential_match_join;",
        ),
        # Composite index for PersonRecord person_id lookups
        # Optimizes: pr.person_id = p.id and pr_all.person_id = p.id
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_personrecord_person_lookup
            ON main_personrecord (person_id, id, data_source, first_name, last_name);
            """,
            "DROP INDEX IF EXISTS idx_personrecord_person_lookup;",
        ),
        # Index for Person table UUID lookups
        # Optimizes: p.uuid lookups in the response
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_person_uuid_lookup
            ON main_person (uuid, id, created, version);
            """,
            "DROP INDEX IF EXISTS idx_person_uuid_lookup;",
        ),
        # ===== INDEXES FOR POST /matches ENDPOINT =====
        # Composite index for MatchGroup version checking and locking
        # Optimizes: WHERE uuid = %s AND version = %s FOR UPDATE
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_matchgroup_version_lock
            ON main_matchgroup (uuid, version, id)
            WHERE matched IS NULL AND deleted IS NULL;
            """,
            "DROP INDEX IF EXISTS idx_matchgroup_version_lock;",
        ),
        # Index for Person UUID and version lookups in match creation
        # Optimizes: Person.objects.filter(uuid=update["uuid"], version=update["version"])
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_person_uuid_version
            ON main_person (uuid, version, id);
            """,
            "DROP INDEX IF EXISTS idx_person_uuid_version;",
        ),
        # Composite index for PersonRecord ID lookups in match updates
        # Optimizes bulk operations on PersonRecord during match creation
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_personrecord_match_operations
            ON main_personrecord (id, person_id, person_updated);
            """,
            "DROP INDEX IF EXISTS idx_personrecord_match_operations;",
        ),
        # Index for MatchEvent creation and tracking
        # Optimizes MatchEvent creation and PersonAction lookups
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_matchevent_creation
            ON main_matchevent (created, type, job_id);
            """,
            "DROP INDEX IF EXISTS idx_matchevent_creation;",
        ),
        # Composite index for PersonAction bulk operations
        # Optimizes bulk inserts during match creation
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_personaction_bulk_ops
            ON main_personaction (match_event_id, person_id, person_record_id, type);
            """,
            "DROP INDEX IF EXISTS idx_personaction_bulk_ops;",
        ),
        # ===== ADDITIONAL PERFORMANCE INDEXES =====
        # Partial index for active MatchGroups (most common query pattern)
        # This covers the most common filtering: WHERE matched IS NULL AND deleted IS NULL
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_matchgroup_active_only
            ON main_matchgroup (id, uuid, created, updated, job_id)
            WHERE matched IS NULL AND deleted IS NULL;
            """,
            "DROP INDEX IF EXISTS idx_matchgroup_active_only;",
        ),
        # Composite index for SplinkResult match probability ordering
        # Optimizes ORDER BY match_probability DESC in potential match queries
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_splinkresult_probability_order
            ON main_splinkresult (match_group_id, match_probability DESC, person_record_l_id, person_record_r_id);
            """,
            "DROP INDEX IF EXISTS idx_splinkresult_probability_order;",
        ),
        # Index for PersonRecord created timestamp ordering
        # Optimizes ORDER BY pr_all.id in potential match queries
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_personrecord_created_order
            ON main_personrecord (person_id, id, created);
            """,
            "DROP INDEX IF EXISTS idx_personrecord_created_order;",
        ),
        # ===== COVERING INDEXES FOR COMMON QUERY PATTERNS =====
        # Covering index for MatchGroup summary queries
        # Includes all fields commonly accessed in potential match summaries
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_matchgroup_covering
            ON main_matchgroup (id, uuid, created, updated, matched, deleted, job_id, version)
            WHERE matched IS NULL AND deleted IS NULL;
            """,
            "DROP INDEX IF EXISTS idx_matchgroup_covering;",
        ),
        # Covering index for PersonRecord essential fields
        # Includes the most commonly accessed fields to avoid table lookups
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_personrecord_covering
            ON main_personrecord (person_id, id, data_source, first_name, last_name, created, person_updated);
            """,
            "DROP INDEX IF EXISTS idx_personrecord_covering;",
        ),
    ]
