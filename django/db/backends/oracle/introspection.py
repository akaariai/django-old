from django.db import QName
from django.db.backends import BaseDatabaseIntrospection
import cx_Oracle
import re

foreign_key_re = re.compile(r"\sCONSTRAINT `[^`]*` FOREIGN KEY \(`([^`]*)`\) REFERENCES `([^`]*)` \(`([^`]*)`\)")

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type objects to Django Field types.
    data_types_reverse = {
        cx_Oracle.CLOB: 'TextField',
        cx_Oracle.DATETIME: 'DateField',
        cx_Oracle.FIXED_CHAR: 'CharField',
        cx_Oracle.NCLOB: 'TextField',
        cx_Oracle.NUMBER: 'DecimalField',
        cx_Oracle.STRING: 'CharField',
        cx_Oracle.TIMESTAMP: 'DateTimeField',
    }

    try:
        data_types_reverse[cx_Oracle.NATIVE_FLOAT] = 'FloatField'
    except AttributeError:
        pass

    try:
        data_types_reverse[cx_Oracle.UNICODE] = 'CharField'
    except AttributeError:
        pass

    def get_field_type(self, data_type, description):
        # If it's a NUMBER with scale == 0, consider it an IntegerField
        if data_type == cx_Oracle.NUMBER and description[5] == 0:
            if description[4] > 11:
                return 'BigIntegerField'
            else:
                return 'IntegerField'
        else:
            return super(DatabaseIntrospection, self).get_field_type(
                data_type, description)

    def get_visible_tables_list(self, cursor):
        "Returns a list of visible tables"
        return self.get_qualified_tables_list(cursor, [self.connection.settings_dict['USER']])

    def get_qualified_tables_list(self, cursor, schemas):
        "Returns a list of table names in the given schemas list."
        default_schema = self.connection.convert_schema(None)
        if default_schema:
            schemas.append(default_schema)
        if not schemas:
            return []
        param_list = ', '.join(['%s']*len(schemas))
        schemas = [s.upper() for s in schemas]
        cursor.execute("""
            SELECT OWNER, TABLE_NAME
              FROM ALL_TABLES WHERE OWNER in (%s)""" % param_list, schemas)
        return [QName(row[0].lower(), row[1].lower(), True)
                for row in cursor.fetchall()]

    def get_table_description(self, cursor, qname):
        "Returns a description of the table, with the DB-API cursor.description interface."
        cursor.execute("SELECT * FROM %s WHERE ROWNUM < 2"
                       % self.connection.ops.qualified_name(qname))
        description = []
        for desc in cursor.description:
            description.append((desc[0].lower(),) + desc[1:])
        return description

    def identifier_converter(self, name):
        return name.lower()

    def qname_converter(self, qname, force_schema=False):
        assert isinstance(qname, QName)
        if qname.db_format and (qname.schema or not force_schema):
            return qname
        schema = self.connection.convert_schema(qname.schema)
        if not schema and force_schema:
            schema = self.connection.settings_dict['USER']
        return QName(schema, qname.table, True)

    def _name_to_index(self, cursor, qname):
        """
        Returns a dictionary of {field_name: field_index} for the given table.
        Indexes are 0-based.
        """
        return dict([(d[0], i) for i, d in enumerate(self.get_table_description(cursor, qname))])

    def get_relations(self, cursor, qname):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        qname = self.qname_converter(qname, force_schema=True)
        schema, table = qname.schema.upper(), qname.table.upper()
        cursor.execute("""
    SELECT ta.column_id - 1, tb.table_name, tb.owner, tb.column_id - 1
    FROM   all_constraints, ALL_CONS_COLUMNS ca, ALL_CONS_COLUMNS cb,
           all_tab_cols ta, all_tab_cols tb
    WHERE  all_constraints.table_name = %s AND
           all_constraints.owner = %s AND
           ta.table_name = %s AND
           ta.owner = %s AND
           ta.column_name = ca.column_name AND
           ca.table_name = %s AND
           ca.owner = %s AND
           all_constraints.constraint_name = ca.constraint_name AND
           all_constraints.r_constraint_name = cb.constraint_name AND
           cb.table_name = tb.table_name AND
           cb.column_name = tb.column_name AND
           ca.position = cb.position""", [table, schema, table, schema,
                                          table, schema])

        relations = {}
        for row in cursor.fetchall():
            relations[row[0]] = (
                row[3], QName(row[2].lower(), row[1].lower(), True))
        return relations

    def get_indexes(self, cursor, qname):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}
        """
        # This query retrieves each index on the given table, including the
        # first associated field name
        # "We were in the nick of time; you were in great peril!"
        qname = self.qname_converter(qname, force_schema=True)
        schema, table = qname.schema.upper(), qname.table.upper()
        # There can be multiple constraints for a given column, and we 
        # are interested if _any_ of them is unique or primary key, hence
        # the group by + max.
        sql = """\
SELECT column_name, max(is_primary_key), max(is_unique)
  FROM (
    SELECT LOWER(all_tab_cols.column_name) AS column_name,
           CASE all_constraints.constraint_type
               WHEN 'P' THEN 1 ELSE 0
           END AS is_primary_key,
           CASE all_indexes.uniqueness
               WHEN 'UNIQUE' THEN 1 ELSE 0
           END AS is_unique
    FROM   all_tab_cols, all_cons_columns, all_constraints, all_ind_columns, all_indexes
    WHERE  all_tab_cols.column_name = all_cons_columns.column_name (+)
      AND  all_tab_cols.table_name = all_cons_columns.table_name (+)
      AND  all_tab_cols.owner = all_cons_columns.owner (+)
      AND  all_cons_columns.constraint_name = all_constraints.constraint_name (+)
      AND  all_cons_columns.owner = all_constraints.owner (+)
      AND  all_constraints.constraint_type (+) = 'P'
      AND  all_ind_columns.column_name (+) = all_tab_cols.column_name
      AND  all_ind_columns.table_name (+) = all_tab_cols.table_name
      AND  all_ind_columns.index_owner (+) = all_tab_cols.owner
      AND  all_indexes.uniqueness (+) = 'UNIQUE'
      AND  all_indexes.index_name (+) = all_ind_columns.index_name
      AND  all_indexes.table_owner (+) = all_ind_columns.index_owner
      AND  all_tab_cols.table_name = %s
      AND  all_tab_cols.owner = %s
    )
GROUP BY column_name
"""
        cursor.execute(sql, [table, schema])
        indexes = {}
        for row in cursor.fetchall():
            indexes[row[0]] = {'primary_key': row[1], 'unique': row[2]}
        return indexes

    def get_schema_list(self, cursor):
        cursor.execute("SELECT USERNAME FROM ALL_USERS")
        return [r[0] for r in cursor.fetchall()]
