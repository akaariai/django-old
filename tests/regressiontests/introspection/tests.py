from __future__ import absolute_import

from functools import update_wrapper

from django.conf import settings
from django.db import connection
from django.test import TestCase, skipUnlessDBFeature, skipIfDBFeature

from .models import Reporter, Article

#
# The introspection module is optional, so methods tested here might raise
# NotImplementedError. This is perfectly acceptable behavior for the backend
# in question, but the tests need to handle this without failing. Ideally we'd
# skip these tests, but until #4788 is done we'll just ignore them.
#
# The easiest way to accomplish this is to decorate every test case with a
# wrapper that ignores the exception.
#
# The metaclass is just for fun.
#

def ignore_not_implemented(func):
    def _inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotImplementedError:
            return None
    update_wrapper(_inner, func)
    return _inner

class IgnoreNotimplementedError(type):
    def __new__(cls, name, bases, attrs):
        for k,v in attrs.items():
            if k.startswith('test'):
                attrs[k] = ignore_not_implemented(v)
        return type.__new__(cls, name, bases, attrs)

class IntrospectionTests(TestCase):
    __metaclass__ = IgnoreNotimplementedError

    def test_table_names(self):
        conv = connection.introspection.table_name_converter
        tl = connection.introspection.all_qualified_names(converted=True)
        self.assertTrue(conv(Reporter._meta.qualified_name) in tl,
                     "'%s' isn't in table_list()." % Reporter._meta.db_table)
        self.assertTrue(conv(Article._meta.qualified_name) in tl,
                     "'%s' isn't in table_list()." % Article._meta.db_table)

    def test_django_table_names(self):
        cursor = connection.cursor()
        tblname = connection.ops.qualified_name((None, 'django_ixn_test_table'))
        cursor.execute('CREATE TABLE %s (id INTEGER);' % tblname)
        tl = connection.introspection.django_table_names()
        cursor.execute("DROP TABLE %s;" % tblname)
        self.assertTrue(tblname not in tl,
                     "django_table_names() returned a non-Django table")

    def test_django_table_names_retval_type(self):
        # Ticket #15216
        cursor = connection.cursor()
        tblname = connection.ops.qualified_name((None, 'django_ixn_test_table'))
        cursor.execute('CREATE TABLE %s (id INTEGER);' % tblname)

        tl = connection.introspection.django_table_names(only_existing=True)
        self.assertIs(type(tl), list)

        tl = connection.introspection.django_table_names(only_existing=False)
        self.assertIs(type(tl), list)

    def test_installed_models(self):
        tables = [(settings.DEFAULT_SCHEMA, Article._meta.db_table),
                  (settings.DEFAULT_SCHEMA, Reporter._meta.db_table)]
        models = connection.introspection.installed_models(tables)
        self.assertEqual(models, set([Article, Reporter]))

    def test_sequence_list(self):
        sequences = connection.introspection.sequence_list()
        schema, table = connection.introspection.table_name_converter(Reporter._meta.qualified_name)
        expected = {'table': table, 'column': 'id',
                    'schema': schema}
        self.assertTrue(expected in sequences,
                     'Reporter sequence not found in sequence_list()')

    def test_get_table_description_names(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Reporter._meta.qualified_name)
        desc = connection.introspection.get_table_description(cursor, tbl)
        self.assertEqual([r[0] for r in desc],
                         [f.column for f in Reporter._meta.fields])

    def test_get_table_description_types(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Reporter._meta.qualified_name)
        desc = connection.introspection.get_table_description(cursor, tbl)
        self.assertEqual(
            [datatype(r[1], r) for r in desc],
            ['IntegerField', 'CharField', 'CharField', 'CharField', 'BigIntegerField']
        )

    # Oracle forces null=True under the hood in some cases (see
    # https://docs.djangoproject.com/en/dev/ref/databases/#null-and-empty-strings)
    # so its idea about null_ok in cursor.description is different from ours.
    @skipIfDBFeature('interprets_empty_strings_as_nulls')
    def test_get_table_description_nullable(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Reporter._meta.qualified_name)
        desc = connection.introspection.get_table_description(cursor, tbl)
        self.assertEqual(
            [r[6] for r in desc],
            [False, False, False, False, True]
        )

    # Regression test for #9991 - 'real' types in postgres
    @skipUnlessDBFeature('has_real_datatype')
    def test_postgresql_real_type(self):
        cursor = connection.cursor()
        tblname = connection.ops.qualified_name((None, 'django_ixn_real_test_table'))
        cursor.execute("CREATE TABLE %s (number REAL);" % tblname)
        desc = connection.introspection.get_table_description(cursor, (None, 'django_ixn_real_test_table'))
        cursor.execute('DROP TABLE %s;' % tblname)
        self.assertEqual(datatype(desc[0][1], desc[0]), 'FloatField')

    def test_get_relations(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Article._meta.qualified_name)
        relations = connection.introspection.get_relations(cursor, tbl)
        rep_tbl = connection.introspection.table_name_converter(Reporter._meta.qualified_name)

        # Older versions of MySQL don't have the chops to report on this stuff,
        # so just skip it if no relations come back. If they do, though, we
        # should test that the response is correct.
        if relations:
            # That's {field_index: (field_index_other_table, other_table)}
            # We have a small problem here: the Reporter model is installed
            # into the default schema (qualified_name[0] == None). The
            # relation introspection is going to see it in that schema, but we
            # do not know what that schema is. So, test everything except the
            # schema.
            # TODO: this testing logic is UGLY!
            schema = connection.convert_schema(Reporter._meta.qname.schema)
            self.assertTrue(3 in relations)
            relations[3] = (relations[3][0], (schema, relations[3][1][1]))
            self.assertEqual(relations, {3: (0, rep_tbl)})

    def test_get_key_columns(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Article._meta.qualified_name)
        rep_tbl = connection.introspection.table_name_converter(Reporter._meta.qualified_name)
        key_columns = connection.introspection.get_key_columns(cursor, tbl)
        self.assertEqual(key_columns, [(u'reporter_id', rep_tbl, u'id')])

    def test_get_primary_key_column(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Article._meta.qualified_name)
        primary_key_column = connection.introspection.get_primary_key_column(cursor, tbl)
        self.assertEqual(primary_key_column, u'id')

    def test_get_indexes(self):
        cursor = connection.cursor()
        tbl = connection.introspection.table_name_converter(Article._meta.qualified_name)
        indexes = connection.introspection.get_indexes(cursor, tbl)
        self.assertEqual(indexes['reporter_id'], {'unique': False, 'primary_key': False})


def datatype(dbtype, description):
    """Helper to convert a data type into a string."""
    dt = connection.introspection.get_field_type(dbtype, description)
    if type(dt) is tuple:
        return dt[0]
    else:
        return dt
