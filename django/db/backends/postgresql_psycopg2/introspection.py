from django.db.backends import BaseDatabaseIntrospection


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        16: 'BooleanField',
        20: 'BigIntegerField',
        21: 'SmallIntegerField',
        23: 'IntegerField',
        25: 'TextField',
        700: 'FloatField',
        701: 'FloatField',
        869: 'GenericIPAddressField',
        1042: 'CharField', # blank-padded
        1043: 'CharField',
        1082: 'DateField',
        1083: 'TimeField',
        1114: 'DateTimeField',
        1184: 'DateTimeField',
        1266: 'TimeField',
        1700: 'DecimalField',
    }

    def get_schema_list(self, cursor):
        cursor.execute("""
            SELECT n.nspname
            FROM pg_catalog.pg_namespace n
            WHERE n.nspname != 'information_schema' OR n.nspname not like 'pg_%s'""")
        return [row[0] for row in cursor.fetchall()]
        
    def get_visible_tables_list(self, cursor):
        """
        Returns a list of all by-default visible table names in the current
        database.
        """
        sql = """
            SELECT n.nspname, c.relname
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'v', '')
                AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
                AND (pg_catalog.pg_table_is_visible(c.oid)"""
        # We must add the default schema to always visible schemas to make
        # things work nicely.
        if self.connection.schema:
            sql += " OR n.nspname = %s)"
            cursor.execute(sql, (self.connection.schema,))
        else:
            cursor.execute(sql + ')')
        return [(row[0], row[1]) for row in cursor.fetchall()]
    
    def get_qualified_tables_list(self, cursor):
        """
        Returns schema qualified names of all tables in the current database.
        """
        cursor.execute("""
            SELECT n.nspname, c.relname
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'v', '')
                AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')""")
        return [(row[0], row[1]) for row in cursor.fetchall()]

    def get_table_description(self, cursor, qualified_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        # As cursor.description does not return reliably the nullable property,
        # we have to query the information_schema (#7783)
        if not qualified_name[0]:
            cursor.execute("""
                SELECT column_name, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s""",
                [qualified_name[1]])
        else:
            cursor.execute("""
                SELECT column_name, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s and table_name = %s""",
                [qualified_name[0], qualified_name[1]])
        null_map = dict(cursor.fetchall())
        cursor.execute(
            "SELECT * FROM %s LIMIT 1" % self.connection.ops.qualified_name(qualified_name))
        try:
         return [tuple([item for item in line[:6]] + [null_map[line[0]]==u'YES'])
            for line in cursor.description]
        except:
            import ipdb; ipdb.set_trace()
            raise

    def get_relations(self, cursor, qualified_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based. The
        other_table will be in qualified format.
        """
        if not qualified_name[0]:
            cursor.execute("""
                SELECT con.conkey, con.confkey, nsp2.nspname, c2.relname
                FROM pg_constraint con, pg_class c1, pg_class c2,
                     pg_namespace nsp2
                WHERE c1.oid = con.conrelid
                    AND c2.oid = con.confrelid
                    AND nsp2.oid = c2.relnamespace
                    AND c1.relname = %s
                    AND con.contype = 'f'""",
                [qualified_name[1]])
        else:
            cursor.execute("""
                SELECT con.conkey, con.confkey, nsp2.nspname, c2.relname
                FROM pg_constraint con, pg_class c1, pg_class c2,
                     pg_namespace nsp1, pg_namespace nsp2
                WHERE c1.oid = con.conrelid
                    AND nsp1.oid = c1.relnamespace
                    AND c2.oid = con.confrelid
                    AND nsp2.oid = c2.relnamespace
                    AND nsp1.nspname = %s
                    AND c1.relname = %s
                    AND con.contype = 'f'""",
                [qualified_name[0], qualified_name[1]])
        relations = {}
        for row in cursor.fetchall():
            # row[0] and row[1] are single-item lists, so grab the single item.
            relations[row[0][0] - 1] = (row[1][0] - 1, (row[2], row[3]))
        return relations

    def get_indexes(self, cursor, qualified_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}
        """
        # This query retrieves each index on the given table, including the
        # first associated field name
        if not qualified_name[0]:
            cursor.execute("""
                SELECT attr.attname, idx.indkey, idx.indisunique, idx.indisprimary
                FROM pg_catalog.pg_class c, pg_catalog.pg_class c2,
                    pg_catalog.pg_index idx, pg_catalog.pg_attribute attr
                WHERE c.oid = idx.indrelid
                    AND idx.indexrelid = c2.oid
                    AND attr.attrelid = c.oid
                    AND attr.attnum = idx.indkey[0]
                    AND c.relname = %s""",
                [qualified_name[1]])
        else:
            cursor.execute("""
                SELECT attr.attname, idx.indkey, idx.indisunique, idx.indisprimary
                FROM pg_catalog.pg_class c, pg_catalog.pg_namespace nsp,
                    pg_catalog.pg_class c2, pg_catalog.pg_index idx,
                    pg_catalog.pg_attribute attr
                WHERE c.oid = idx.indrelid
                    AND nsp.oid = c.relnamespace
                    AND idx.indexrelid = c2.oid
                    AND attr.attrelid = c.oid
                    AND attr.attnum = idx.indkey[0]
                    AND nsp.nspname = %s AND c.relname = %s""",
                [qualified_name[0], qualified_name[1]])
        indexes = {}
        for row in cursor.fetchall():
            # row[1] (idx.indkey) is stored in the DB as an array. It comes out as
            # a string of space-separated integers. This designates the field
            # indexes (1-based) of the fields that have indexes on the table.
            # Here, we skip any indexes across multiple fields.
            if ' ' in row[1]:
                continue
            indexes[row[0]] = {'primary_key': row[3], 'unique': row[2]}
        return indexes

    def table_name_converter(self, name, plain=False):
        if isinstance(name, tuple):
            return (name[0] or self.connection.settings_dict['SCHEMA']), name[1]
        return name
