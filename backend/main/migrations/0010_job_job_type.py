# Generated manually

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0009_remove_job_s3_uri_state'),
    ]

    operations = [
        migrations.AddField(
            model_name='job',
            name='job_type',
            field=models.TextField(choices=[('import-person-records', 'import-person-records'), ('export-potential-matches', 'export-potential-matches')], default='import-person-records'),
        ),
    ]
