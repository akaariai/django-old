from django.db import models

class SameName1(models.Model):
    txt = models.TextField(null=True)

    class Meta:
        db_table = 'stn1'

class SameName2(models.Model):
    fk = models.ForeignKey(SameName1)

    class Meta:
        db_table = 'stn2'
        db_schema = 'schema1'

class SameName3(models.Model):
    fk = models.ForeignKey(SameName1)

    class Meta:
        db_table = 'stn3'
        db_schema = 'schema2'

class M2MTable(models.Model):
    m2m = models.ManyToManyField(SameName1)

    class Meta:
        db_schema = 'schema1'

class M2MTable2(models.Model):
    m2m = models.ManyToManyField(SameName2, db_schema='schema3')

    class Meta:
        db_schema = 'schema2'
