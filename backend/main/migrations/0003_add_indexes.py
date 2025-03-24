from django.db import migrations, models

from main.models import PersonRecord


class Migration(migrations.Migration):
    person_record_table = PersonRecord._meta.db_table

    dependencies = [
        ("main", "0002_job_reason"),
    ]

    operations = [
        migrations.RunSQL(
            "create extension if not exists pg_trgm", migrations.RunSQL.noop
        ),
        migrations.RunSQL(
            sql=f"create index main_personrecord_fname_idx on {person_record_table} using gin (first_name gin_trgm_ops);",
            reverse_sql="drop index if exists main_personrecord_fname_idx;",
        ),
        migrations.RunSQL(
            sql=f"create index main_personrecord_lname_idx on {person_record_table} using gin (last_name gin_trgm_ops);",
            reverse_sql="drop index if exists main_personrecord_lname_idx;",
        ),
        migrations.RunSQL(
            sql=f"create index main_personrecord_dob_idx on {person_record_table} using gin (birth_date gin_trgm_ops);",
            reverse_sql="drop index if exists main_personrecord_dob_idx;",
        ),
        migrations.RunSQL(
            sql=f"create index main_personrecord_pid_idx on {person_record_table} using btree (cast(person_id as text));",
            reverse_sql="drop index if exists main_personrecord_pid_idx;",
        ),
        migrations.RunSQL(
            sql=f"create index main_personrecord_spid_idx on {person_record_table} using btree (cast(source_person_id as text));",
            reverse_sql="drop index if exists main_personrecord_spid_idx;",
        ),
        migrations.AddIndex(
            model_name="personrecord",
            index=models.Index(
                fields=["data_source"], name="main_person_data_so_4bea85_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="matchgroup",
            index=models.Index(
                fields=["deleted"], name="main_matchg_deleted_d15f4e_idx"
            ),
        ),
        migrations.AddIndex(
            model_name="matchgroup",
            index=models.Index(
                fields=["matched"], name="main_matchg_matched_a837a2_idx"
            ),
        ),
    ]
