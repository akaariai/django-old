from django.db import models
"""

class SameTableName1(models.Model):
    pass

    class Meta:
        db_table = 'stn'
        db_schema = 'schema1'

class SameTableName2(models.Model):
    fk = models.ForeignKey(SameTableName1)

    class Meta:
        db_table = 'stn'
        db_schema = 'schema2'
"""
