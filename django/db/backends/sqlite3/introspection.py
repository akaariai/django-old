import re
from django.db.backends import BaseDatabaseIntrospection

# This light wrapper "fakes" a dictionary interface, because some SQLite data
# types include variables in them -- e.g. "varchar(30)" -- and can't be matched
# as a simple dictionary lookup.
class FlexibleFieldLookupDict(object):
    # Maps SQL types to Django Field types. Some of the SQL types have multiple
    # entries here because SQLite allows for anything and doesn't normalize the
    # field type; it uses whatever was given.
    base_data_types_reverse = {
        'bool': 'BooleanField',
        'boolean': 'BooleanField',
        'smallint': 'SmallIntegerField',
        'smallint unsigned': 'PositiveSmallIntegerField',
        'smallinteger': 'SmallIntegerField',
        'int': 'IntegerField',
        'integer': 'IntegerField',
        'bigint': 'BigIntegerField',
        'integer unsigned': 'PositiveIntegerField',
        'decimal': 'DecimalField',
        'real': 'FloatField',
        'text': 'TextField',
        'char': 'CharField',
        'date': 'DateField',
        'datetime': 'DateTimeField',
        'time': 'TimeField',
    }

    def __getitem__(self, key):
        key = key.lower()
        try:
            return self.base_data_types_reverse[key]
        except KeyError:
            import re
            m = re.search(r'^\s*(?:var)?char\s*\(\s*(\d+)\s*\)\s*$', key)
            if m:
                return ('CharField', {'max_length': int(m.group(1))})
            raise KeyError

class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = FlexibleFieldLookupDict()

    def get_visible_tables_list(self, cursor):
        "Returns a list of table names in the current database"
        # Skip the sqlite_sequence system table used for autoincrement key
        # generation.
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND NOT name='sqlite_sequence'
            ORDER BY name""")
        return [(None, row[0]) for row in cursor.fetchall()]

    def get_table_description(self, cursor, qualified_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        return [(info['name'], info['type'], None, None, None, None,
                 info['null_ok']) for info in self._table_info(cursor, qualified_name[1])]

    def get_relations(self, cursor, qualified_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        table_name = qualified_name[1]

        # Dictionary of relations to return
        relations = {}

        # Schema for this table
        cursor.execute("SELECT sql FROM sqlite_master WHERE tbl_name = %s AND type = %s", [table_name, "table"])
        results = cursor.fetchone()[0].strip()
        results = results[results.index('(')+1:results.rindex(')')]

        # Walk through and look for references to other tables. SQLite doesn't
        # really have enforced references, but since it echoes out the SQL used
        # to create the table we can look for REFERENCES statements used there.
        for field_index, field_desc in enumerate(results.split(',')):
            field_desc = field_desc.strip()
            if field_desc.startswith("UNIQUE"):
                continue

            m = re.search('references (.*) \(["|](.*)["|]\)', field_desc, re.I)
            if not m:
                continue

            table, column = [s.strip('"') for s in m.groups()]

            cursor.execute("SELECT sql FROM sqlite_master WHERE tbl_name = %s", [table])
            result = cursor.fetchall()[0]
            other_table_results = result[0].strip()
            li, ri = other_table_results.index('('), other_table_results.rindex(')')
            other_table_results = other_table_results[li+1:ri]


            for other_index, other_desc in enumerate(other_table_results.split(',')):
                other_desc = other_desc.strip()
                if other_desc.startswith('UNIQUE'):
                    continue

                name = other_desc.split(' ', 1)[0].strip('"')
                if name == column:
                    relations[field_index] = (other_index, (None, table))
                    break

        return relations

    def get_key_columns(self, cursor, qualified_name):
        """
        Returns a list of (column_name, referenced_table_name, referenced_column_name) for all
        key columns in given table.
        """
        table_name = qualified_name[1]
        key_columns = []

        # Schema for this table
        cursor.execute("SELECT sql FROM sqlite_master WHERE tbl_name = %s AND type = %s", [table_name, "table"])
        results = cursor.fetchone()[0].strip()
        results = results[results.index('(')+1:results.rindex(')')]

        # Walk through and look for references to other tables. SQLite doesn't
        # really have enforced references, but since it echoes out the SQL used
        # to create the table we can look for REFERENCES statements used there.
        for field_index, field_desc in enumerate(results.split(',')):
            field_desc = field_desc.strip()
            if field_desc.startswith("UNIQUE"):
                continue

            m = re.search('"(.*)".*references (.*) \(["|](.*)["|]\)', field_desc, re.I)
            if not m:
                continue

            # This will append (column_name, referenced_table_name, referenced_column_name) to key_columns
            add = tuple([s.strip('"') for s in m.groups()])
            add = add[0], (None, add[1]), add[2]
            key_columns.append(add)

        return key_columns

    def get_indexes(self, cursor, qualified_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}
        """
        table_name = qualified_name[1]
        indexes = {}
        for info in self._table_info(cursor, table_name):
            indexes[info['name']] = {'primary_key': info['pk'] != 0,
                                     'unique': False}
        cursor.execute('PRAGMA index_list(%s)' % self.connection.ops.quote_name(table_name))
        # seq, name, unique
        for index, unique in [(field[1], field[2]) for field in cursor.fetchall()]:
            if not unique:
                continue
            cursor.execute('PRAGMA index_info(%s)' % self.connection.ops.quote_name(index))
            info = cursor.fetchall()
            # Skip indexes across multiple fields
            if len(info) != 1:
                continue
            name = info[0][2] # seqno, cid, name
            indexes[name]['unique'] = True
        return indexes

    def get_primary_key_column(self, cursor, qualified_name):
        """
        Get the column name of the primary key for the given table.
        """
        table_name = qualified_name[1]
        # Don't use PRAGMA because that causes issues with some transactions
        cursor.execute("SELECT sql FROM sqlite_master WHERE tbl_name = %s AND type = %s", [table_name, "table"])
        results = cursor.fetchone()[0].strip()
        results = results[results.index('(')+1:results.rindex(')')]
        for field_desc in results.split(','):
            field_desc = field_desc.strip()
            m = re.search('"(.*)".*PRIMARY KEY$', field_desc)
            if m:
                return m.groups()[0]
        return None

    def _table_info(self, cursor, name):
        cursor.execute('PRAGMA table_info(%s)' % self.connection.ops.quote_name(name))
        # cid, name, type, notnull, dflt_value, pk
        return [{'name': field[1],
                 'type': field[2],
                 'null_ok': not field[3],
                 'pk': field[5]     # undocumented
                 } for field in cursor.fetchall()]

    def table_name_converter(self, name):
        if isinstance(name, tuple):
            return None, name[1]
        else:
            return name
