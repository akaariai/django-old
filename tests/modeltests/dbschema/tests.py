from __future__ import absolute_import

from .models import SameName1, SameName2, M2MTable

from django.test import TestCase, skipUnlessDBFeature

class SchemaTests(TestCase):
    @skipUnlessDBFeature("supports_schemas")
    def test_create(self):
        sn1 = SameName1.objects.create()
        self.assertEqual(SameName2.objects.count(), 0)
        SameName2.objects.create(fk=sn1)
        self.assertEqual(SameName1.objects.count(), 1)

    @skipUnlessDBFeature("supports_schemas")
    def test_fk(self):
        sn1 = SameName1.objects.create()
        sn2 = SameName1.objects.create()
        SameName2.objects.create(fk=sn1)
        SameName2.objects.create(fk=sn1)
        SameName2.objects.create(fk=sn2)
        self.assertEqual(SameName2.objects.filter(fk=sn1).count(), 2)
        self.assertEqual(SameName2.objects.filter(fk=sn2).count(), 1)
        self.assertEqual(SameName2.objects.select_related('fk').order_by('fk__pk')[0].fk.pk, sn1.pk)
        self.assertEqual(SameName2.objects.order_by('fk__pk')[0].fk.pk, sn1.pk)
    
    @skipUnlessDBFeature("supports_schemas")
    def test_m2m(self):
        sn1 = SameName1.objects.create()
        sn2 = SameName1.objects.create()
        m1 = M2MTable.objects.create()
        m1.m2m.add(sn1)
        m1.m2m.add(sn2)
        M2MTable.objects.create()
        m1 = M2MTable.objects.filter(m2m__in=[sn1, sn2])[0]
        self.assertEquals(list(m1.m2m.order_by('pk')), [sn1, sn2])

