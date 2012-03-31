from django.db.backends import BaseDatabaseIntrospection
from MySQLdb import ProgrammingError, OperationalError
from MySQLdb.constants import FIELD_TYPE
import re

foreign_key_re = re.compile(r"\sCONSTRAINT `[^`]*` FOREIGN KEY \(`([^`]*)`\) REFERENCES `([^`]*)` \(`([^`]*)`\)")

class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = {
        FIELD_TYPE.BLOB: 'TextField',
        FIELD_TYPE.CHAR: 'CharField',
        FIELD_TYPE.DECIMAL: 'DecimalField',
        FIELD_TYPE.NEWDECIMAL: 'DecimalField',
        FIELD_TYPE.DATE: 'DateField',
        FIELD_TYPE.DATETIME: 'DateTimeField',
        FIELD_TYPE.DOUBLE: 'FloatField',
        FIELD_TYPE.FLOAT: 'FloatField',
        FIELD_TYPE.INT24: 'IntegerField',
        FIELD_TYPE.LONG: 'IntegerField',
        FIELD_TYPE.LONGLONG: 'BigIntegerField',
        FIELD_TYPE.SHORT: 'IntegerField',
        FIELD_TYPE.STRING: 'CharField',
        FIELD_TYPE.TIMESTAMP: 'DateTimeField',
        FIELD_TYPE.TINY: 'IntegerField',
        FIELD_TYPE.TINY_BLOB: 'TextField',
        FIELD_TYPE.MEDIUM_BLOB: 'TextField',
        FIELD_TYPE.LONG_BLOB: 'TextField',
        FIELD_TYPE.VAR_STRING: 'CharField',
    }

    def get_visible_tables_list(self, cursor):
        "Returns a list of visible tables"
        return self.get_qualified_tables_list(cursor, [])

    def get_qualified_tables_list(self, cursor, schemas):
        schemas = list(schemas)
        schemas = [self.connection.ops.schema_to_test_schema(s) for s in schemas]
        schemas.append(self.connection.get_def_schema(None))
        param_list = ', '.join(['%s']*len(schemas))
        cursor.execute("""
            SELECT table_schema, table_name
              FROM information_schema.tables
             WHERE table_schema in (%s)""" % param_list, schemas)
        ret = list(cursor.fetchall())
        return ret

    def get_table_description(self, cursor, qualified_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        cursor.execute("SELECT * FROM %s LIMIT 1" % self.connection.ops.qualified_name(qualified_name))
        return cursor.description

    def _name_to_index(self, cursor, qualified_name):
        """
        Returns a dictionary of {field_name: field_index} for the given table.
        Indexes are 0-based.
        """
        return dict([(d[0], i) for i, d in enumerate(self.get_table_description(cursor, qualified_name))])

    def get_relations(self, cursor, qualified_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        my_field_dict = self._name_to_index(cursor, qualified_name)
        constraints = self.get_key_columns(cursor, qualified_name)
        relations = {}
        for my_fieldname, other_table, other_field in constraints:
            other_field_index = self._name_to_index(cursor, other_table)[other_field]
            my_field_index = my_field_dict[my_fieldname]
            relations[my_field_index] = (other_field_index, other_table)
        return relations

    def get_key_columns(self, cursor, qualified_name):
        """
        Returns a list of
            (column_name,
            (reference_table_schema, referenced_table_name),
            referenced_column_name)
        for all key columns in given table.
        """
        key_columns = []
        schema = qualified_name[0] or self.connection.schema or self.connection.settings_dict['NAME']
        schema = self.connection.ops.schema_to_test_schema(schema)
        qualified_name = schema, qualified_name[1]
        try:
            cursor.execute("""
                SELECT column_name, referenced_table_schema, referenced_table_name, referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = %s
                    AND table_name = %s
                    AND referenced_table_name IS NOT NULL
                    AND referenced_column_name IS NOT NULL""", qualified_name)
            for row in cursor.fetchall():
                key_columns.append((row[0], (row[1], row[2]), row[3]))
        except (ProgrammingError, OperationalError):
            # Fall back to "SHOW CREATE TABLE", for previous MySQL versions.
            # Go through all constraints and save the equal matches.
            cursor.execute("SHOW CREATE TABLE %s" % self.connection.ops.quote_name(qualified_name))
            for row in cursor.fetchall():
                pos = 0
                while True:
                    match = foreign_key_re.search(row[1], pos)
                    if match == None:
                        break
                    pos = match.end()
                    groups = match.groups()
                    tblname = groups[1]
                    if '.' in tblname:
                        tblname = tblname.split('.')
                    else:
                        tblname = None, tblname
                    key_columns.append((groups[0], tblname, groups[2]))
        return key_columns

    def get_primary_key_column(self, cursor, qualified_name):
        """
        Returns the name of the primary key column for the given table
        """
        for column in self.get_indexes(cursor, qualified_name).iteritems():
            if column[1]['primary_key']:
                return column[0]
        return None

    def get_indexes(self, cursor, qualified_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}
        """
        cursor.execute("SHOW INDEX FROM %s" % self.connection.ops.qualified_name(qualified_name))
        indexes = {}
        for row in cursor.fetchall():
            indexes[row[4]] = {'primary_key': (row[2] == 'PRIMARY'), 'unique': not bool(row[1])}
        return indexes

    def get_schema_list(self, cursor):
        cursor.execute("SHOW DATABASES")
        return [r[0] for r in cursor.fetchall()]

    def table_name_converter(self, name, plain=False):
        # In schema-qualified case we need to give back the exact same
        # format as qualified_name gives. In plain case however we use
        # the given name as is. 
        if isinstance(name, tuple):
            schema = self.connection.get_def_schema(name[0])
            return schema, name[1]
        else:
            return name
