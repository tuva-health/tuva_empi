import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("main", "0003_add_indexes"),
    ]

    operations = [
        migrations.AlterField(
            model_name="matchevent",
            name="job",
            field=models.ForeignKey(
                null=True, on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
            ),
        ),
        migrations.AlterField(
            model_name="person",
            name="job",
            field=models.ForeignKey(
                null=True, on_delete=django.db.models.deletion.DO_NOTHING, to="main.job"
            ),
        ),
    ]
