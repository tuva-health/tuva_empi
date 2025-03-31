from django.contrib.postgres.functions import TransactionNow
from django.db import models

MATCHING_SERVICE_LOCK_ID = 100
MATCHING_JOB_LOCK_ID = 200
MATCH_UPDATE_LOCK_ID = 300

TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS.USTZH:TZM'


class UserRole(models.TextChoices):
    admin = "admin"
    member = "member"


class User(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField(db_default=TransactionNow())
    updated = models.DateTimeField(db_default=TransactionNow())
    # External identity provider user ID
    # In AWS cognito, the sub attribute may or may not be a globally unique user identifier (e.g. unique between user pools)
    # It is a UUID, so while practically globally unique, it may not have a verified constraint.
    # Their docs say: "The sub attribute is a unique user identifier within each user pool."
    # https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-settings-attributes.html#cognito-user-pools-standard-attributes
    idp_user_id = models.TextField(
        unique=True
    )  # unique implies that a btree index is created
    role = models.TextField(choices=UserRole, null=True)


class Config(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField(db_default=TransactionNow())
    potential_match_threshold = models.FloatField()
    auto_match_threshold = models.FloatField()
    splink_settings = models.JSONField()


class JobStatus(models.TextChoices):
    new = "new"
    succeeded = "succeeded"
    failed = "failed"


class Job(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField(db_default=TransactionNow())
    updated = models.DateTimeField(db_default=TransactionNow())
    config = models.ForeignKey(Config, on_delete=models.DO_NOTHING)
    s3_uri = models.TextField()
    status = models.TextField(choices=JobStatus)
    reason = models.TextField(null=True)


class PersonRecordStaging(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField()
    job = models.ForeignKey(Job, on_delete=models.DO_NOTHING)
    # This is used for joining with Persons during bulk insert and may not represent the
    # order of the rows in the original file.
    row_number = models.BigIntegerField(null=True)
    sha256 = models.BinaryField(null=True)
    data_source = models.TextField()
    source_person_id = models.TextField()
    first_name = models.TextField()
    last_name = models.TextField()
    sex = models.TextField()
    race = models.TextField()
    birth_date = models.TextField()
    death_date = models.TextField()
    social_security_number = models.TextField()
    address = models.TextField()
    city = models.TextField()
    state = models.TextField()
    zip_code = models.TextField()
    county = models.TextField()
    phone = models.TextField()

    class Meta:
        indexes = [
            # Name is required when using condition. Apparently Django limits explicit Index names to 30 characters,
            # even though implicit names are longer.
            models.Index(
                fields=["sha256"],
                name="main_personrecordstg_sha256",
                # Only index non null values
                condition=models.Q(sha256__isnull=False),
            ),
            models.Index(
                fields=["row_number"],
                name="main_personrecordstg_rownum",
                condition=models.Q(sha256__isnull=False),
            ),
        ]


class Person(models.Model):
    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(unique=True)  # unique implies that a btree index is created
    created = models.DateTimeField()
    updated = models.DateTimeField()
    job = models.ForeignKey(Job, null=True, on_delete=models.DO_NOTHING)
    version = models.BigIntegerField(db_default=1)
    deleted = models.DateTimeField(null=True)
    record_count = models.BigIntegerField()

    class Meta:
        constraints = [
            models.CheckConstraint(
                check=models.Q(record_count__gte=0), name="record_count_gte_zero"
            )
        ]


class PersonRecord(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField()
    # We use the Job to determine which records are new and need to be compared in Splink.
    # We only compare new records with new records and new records with old record.
    # We don't want to compare old records with other old records that have already been
    # compared.
    job = models.ForeignKey(Job, on_delete=models.DO_NOTHING)
    person = models.ForeignKey(Person, on_delete=models.DO_NOTHING)
    person_updated = models.DateTimeField()
    matched_or_reviewed = models.DateTimeField(null=True)
    sha256 = models.BinaryField()
    data_source = models.TextField()
    source_person_id = models.TextField()
    first_name = models.TextField()
    last_name = models.TextField()
    sex = models.TextField()
    race = models.TextField()
    birth_date = models.TextField()
    death_date = models.TextField()
    social_security_number = models.TextField()
    address = models.TextField()
    city = models.TextField()
    state = models.TextField()
    zip_code = models.TextField()
    county = models.TextField()
    phone = models.TextField()

    class Meta:
        indexes = [
            models.Index(fields=["sha256"]),
            models.Index(fields=["data_source"]),
        ]


class PersonRecordNote(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField()
    updated = models.DateTimeField()
    person_record = models.ForeignKey(PersonRecord, on_delete=models.DO_NOTHING)
    note = models.TextField()
    # External user ID
    author = models.TextField()


class MatchGroup(models.Model):
    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(unique=True)  # unique implies that a btree index is created
    created = models.DateTimeField()
    updated = models.DateTimeField()
    # Currently, when we run a new Job, we replace unmatched MatchGroups instead of updating
    # them and soft-delete the old MatchGroups. Some MatchGroups are partially matched,
    # meaning they have MatchActions that reference them. We don't want to fully delete those
    # since then they will no longer relate to SplinkResults via a MatchGroup. Additionally,
    # in order to undo manual-matches and return the MatchGroup to it's original state, we
    # need to keep old MatchGroups.
    deleted = models.DateTimeField(null=True)
    job = models.ForeignKey(Job, on_delete=models.DO_NOTHING)
    version = models.BigIntegerField(db_default=1)
    matched = models.DateTimeField(null=True)

    class Meta:
        indexes = [
            models.Index(fields=["uuid"]),
            models.Index(fields=["deleted"]),
            models.Index(fields=["matched"]),
        ]


class MatchEventType(models.TextChoices):
    # Represents the assignment of new Persons to new PersonRecords, performed automatically
    # by the system.
    new_ids = "new-ids"
    # Represents the automatic matching of PersonRecords performed by the system.
    auto_matches = "auto-matches"
    # Represents the manual matching of PersonRecords performed by a user operator.
    manual_match = "manual-match"
    # Represents the splitting of an existing Person into two or more Persons.
    person_split = "person-split"


class MatchEvent(models.Model):
    # Events are sequentially ordered by id (asc)
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField()
    job = models.ForeignKey(Job, null=True, on_delete=models.DO_NOTHING)
    type = models.TextField(choices=MatchEventType)


class PersonActionType(models.TextChoices):
    add_record = "add-record"
    remove_record = "remove-record"
    review = "review"


class PersonAction(models.Model):
    # Actions are sequentially ordered by id (asc)
    id = models.BigAutoField(primary_key=True)
    match_event = models.ForeignKey(MatchEvent, on_delete=models.DO_NOTHING)
    # The active Match Group (if any) related to the Person at the time
    match_group = models.ForeignKey(MatchGroup, on_delete=models.DO_NOTHING, null=True)
    # The person we are acting upon
    person = models.ForeignKey(Person, on_delete=models.DO_NOTHING)
    person_record = models.ForeignKey(PersonRecord, on_delete=models.DO_NOTHING)
    type = models.TextField(choices=PersonActionType)
    # Internal user ID
    performed_by = models.ForeignKey(User, on_delete=models.DO_NOTHING, null=True)


class SplinkResult(models.Model):
    id = models.BigAutoField(primary_key=True)
    created = models.DateTimeField()
    job = models.ForeignKey(Job, on_delete=models.DO_NOTHING)
    match_group = models.ForeignKey(MatchGroup, on_delete=models.DO_NOTHING)
    match_group_updated = models.DateTimeField()
    match_weight = models.FloatField()
    match_probability = models.FloatField()
    person_record_l = models.ForeignKey(
        PersonRecord, on_delete=models.DO_NOTHING, related_name="splinkresult_l_set"
    )
    person_record_r = models.ForeignKey(
        PersonRecord, on_delete=models.DO_NOTHING, related_name="splinkresult_r_set"
    )
    # If we are loading all raw results to S3, we might not even need this.
    # We can see if we ever use it in the application.
    data = models.JSONField()


class MatchGroupActionType(models.TextChoices):
    # Result is added to Match Group
    add_result = "add-result"
    # Result is removed from Match Group
    remove_result = "remove-result"
    # Only a Person related to the Match Group is updated. Removing Results can involve
    # removing/adding Persons depending on which Results are involved, but we don't add an
    # update_person action for each add_result/remove_result action. add_result/remove_result
    # implies there is a potential update_person action. It's necessary to track this action so
    # that we can increase the MatchGroup version when undoing events. This action is also
    # somewhat redundant, since PersonAction tracks the related MatchGroup when adding Records
    # to/from a Person. However, tracking it here is more explicit.
    update_person = "update-person"
    # This Match Group is matched. This action is also somewhat redundant, since MatchGroup
    # itself contains a matched field. However, tracking it here ties it to the MatchEvent
    # explicitly.
    match = "match"


class MatchGroupAction(models.Model):
    # Actions are sequentially ordered by id (asc)
    id = models.BigAutoField(primary_key=True)
    match_event = models.ForeignKey(MatchEvent, on_delete=models.DO_NOTHING)
    # The Match Group we are acting upon
    match_group = models.ForeignKey(MatchGroup, on_delete=models.DO_NOTHING, null=True)
    splink_result = models.ForeignKey(
        SplinkResult, on_delete=models.DO_NOTHING, null=True
    )
    type = models.TextField(choices=MatchGroupActionType)
    # Internal user ID
    performed_by = models.ForeignKey(User, on_delete=models.DO_NOTHING, null=True)
