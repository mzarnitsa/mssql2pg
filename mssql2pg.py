import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import binascii
import re
import codecs
import getpass

class MsSql2Pg:
    def __init__(self):
        self.param_sql_session = None
        self.param_output_file = None
        self.param_destination_database = None
        self.param_exclude_schemas = None
        self.param_max_record_count = None
        self.param_underscore_identifiers = False

        self.schemas = None
        self.tables = None
        self.columns = None
        self.sequences = None
        self.constraints_pk_uk = None
        self.constraints_check = None
        self.constraints_fk = None
        self.indexes = None

    def read_command_line_params(self):
        parser = argparse.ArgumentParser(description='''
Convert Microsoft SQL Server database into PostgreSQL.
Produces .sql script that can be executed with psql.
        ''')

        parser.add_argument('host_name', help='SQL Server host name')
        parser.add_argument('database_name', help='Source database name')
        parser.add_argument('login_name', help='Login name')

        parser.add_argument('-p', '--password', dest='password', help='Password for the login_name')

        parser.add_argument('-d', '--destination-database', dest='destination_database', default='',
                            help='If not provided, destination database name will be the same as source.')

        parser.add_argument('-f', '--file', dest='output_file_name', default='',
                            help='If not provided, script will be printed to standard output.')

        parser.add_argument('-u', '--underscore', action='store_true', default=False, dest='underscore_identifiers',
                            help='Convert CamelCase into underscored_identifiers (schemas, tables and columns).')

        parser.add_argument('-n', '--limit_records', dest='record_count', default=float("inf"), type=int,
                            help='For test runs, process only provided number of records per table.\n' +
                                 'WARNING: foreign keys may not import properly.')

        parser.add_argument('-x', '--exclude-schemas', dest='exclude_schemas', default='',
                            help='Comma separated (no spaces) list of schemas that will be excluded from export.' +
                                 ' If not provided, all schemas will be processed.\n')

        args = parser.parse_args()

        if args.password is None:
            args.password = getpass.getpass('Password:')

        connection_string = 'mssql+pymssql://{}:{}@{}/{}'.format(
            args.login_name, args.password, args.host_name, args.database_name)
        engine = create_engine(connection_string)

        self.param_sql_session = sessionmaker(bind=engine, autocommit=True)()
        self.param_output_file = args.output_file_name

        if args.destination_database != '':
            self.param_destination_database = args.destination_database
        else:
            self.param_destination_database = args.database_name

        if args.exclude_schemas != '':
            self.param_exclude_schemas = args.exclude_schemas.split(',')
        else:
            self.param_exclude_schemas = []

        self.param_underscore_identifiers = args.underscore_identifiers
        self.param_max_record_count = args.record_count

    def run(self):
        self.read_command_line_params()

        if self.param_output_file is not None:
            try:
                file_name = self.param_output_file
                self.param_output_file = codecs.open(self.param_output_file, 'w', 'utf-8')
            except Exception as e:
                raise SystemExit('Error opening file {}: {}'.format(file_name, e))

        try:
            try:
                self.output_progress('reading schemas')
                self.schemas = self.read_schemas()
                self.output_progress('reading tables')
                self.tables = self.read_tables()
                self.output_progress('reading columns')
                self.columns = self.read_columns()
                self.output_progress('reading computed columns')
                self.read_computed_columns(self.columns)
                self.output_progress('reading identity columns')
                self.sequences = self.read_identity_columns()
                self.output_progress('reading primary, unique key constraints')
                self.constraints_pk_uk = self.read_constraints_pk_uk()
                self.output_progress('reading check constraints')
                self.constraints_check = self.read_constraints_check()
                self.output_progress('reading foreign key constraints')
                self.constraints_fk = self.read_constraints_fk()
                self.output_progress('reading indexes')
                self.indexes = self.read_indexes()

                self.output_progress('writing database')
                self.output_database()
                self.output_progress('writing schemas')
                self.output_schemas()
                self.output_progress('writing sequences')
                self.output_sequences()
                self.output_progress('writing tables')
                self.output_tables()

                self.output_progress('writing data')
                self.output_data()

                self.output_progress('writing sequence start values')
                self.output_sequences_start_values()

                self.output_progress('writing indexes')
                self.output_indexes()

                self.output_progress('writing foreign key constraints')
                self.output_fk_constraints()
            finally:
                self.param_sql_session.close()
        finally:
            if self.param_output_file is not None:
                self.param_output_file.close()

    #############################################################################
    # Translation Functions

    def underscore(self, name):
        result = name

        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        result = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

        while result.find('__') > 0:
            result = result.replace('__', '_')

        return result

    def translate_a_name(self, name):
        #64 is default identifier limit in PostgreSQL
        result = name
        if (
            len(set(result) & set(' .-&/()\\?\'')) > 0
            or result[0] in '0123456789$'
            or result.lower() in ('left', 'constraint', 'order', 'group')
        ):
            result = '"{}"'.format(result[0:64-2])
        else:
            if self.param_underscore_identifiers:
                result = self.underscore(result)[0:64]
            else:
                result = result[0:64]

        return result

    def translate_table_name(self, schema, table):
        if schema == 'dbo':
            result = self.translate_a_name(table)
        else:
            result = '{}.{}'.format(self.translate_a_name(schema), self.translate_a_name(table))

        return result

    def translate_column_type(self, column):
        column_type = column['type'].lower()

        if column_type == 'varchar' or column_type == 'nvarchar':
            if column['char_length'] != -1:
                result = 'VARCHAR({})'.format(column['char_length'])
            else:
                result = 'TEXT'
        elif column_type == 'char' or column_type == 'nchar':
            if column['char_length'] != -1:
                result = 'CHAR({})'.format(column['char_length'])
            else:
                result = 'TEXT'
        elif column_type == 'decimal':
            result = 'NUMERIC({}, {})'.format(column['precision'], column['scale'])
        elif column_type == 'ntext':
            result = 'TEXT'
        elif column_type == 'bit':
            result = 'BOOLEAN'
        elif column_type == 'datetime' or column_type == 'smalldatetime':
            result = 'TIMESTAMP'
        elif column_type == 'uniqueidentifier':
            result = 'UUID'
        elif column_type == 'image':
            result = 'BYTEA'
        elif column_type == 'varbinary':
            result = 'BYTEA'
        elif column_type == 'int':
            result = 'INT'
        elif column_type == 'tinyint':
            result = 'SMALLINT'
        else:
            result = column['type']

        return result

    def translate_default(self, column_type, default):
        if default is None:
            result = None
        else:
            result = default.lower().strip()

            if len(result) == 0:
                result = None
            else:
                while result[0] == '(':
                    result = result[1:len(result)-1].strip()

                if result == 'null':
                    result = None
                elif column_type == 'BOOLEAN':
                    if result == '1':
                        result = 'True'
                    elif result == '0':
                        result = 'False'
                if column_type == 'TIMESTAMP' or column_type == 'DATE' or column_type == 'TIME':
                    if result == 'getdate()':
                        result = 'now()'
                    elif result == 'dateadd(hour,(12),getdate())':
                        result = "now() + interval '12 hours'"
                if column_type[0:6] == 'VARCHAR' or column_type[0:4] == 'CHAR' or column_type[0:4] == 'TEXT':
                    if result[0] == 'n':
                        result = result[1:len(result)]
                if column_type == 'UUID':
                    if result == 'newid()':
                        result = 'uuid_generate_v4()'

        return result

    def translate_check_constraint(self, table):
        result = table
        result = result.replace('[', '')
        result = result.replace(']', '')

        return result

    def translate_data(self, data, data_type):
        data_type = data_type.upper()
        if data is None:
            result = '\\N'
        elif 'VARCHAR' in data_type or 'CHAR' in data_type or 'TEXT' in data_type:
            result = data
            result = result.replace('\\', '\\\\')
            result = result.replace('\n', '\\n')
            result = result.replace('\r', '\\r')
            result = result.replace('\t', '\\t')
            result = result.replace('\'', '\\\'')
            result = result.replace('\"', '\\\"')

            result = result.replace('\a', '\\a')
            result = result.replace('\b', '\\b')
            result = result.replace('\f', '\\f')
            result = result.replace('\v', '\\v')
        elif 'BYTEA' in data_type:
            result = str(binascii.hexlify(data))
        else:
            result = str(data)

        return result

    #############################################################################
    #  Reading Data

    def read_schemas(self):
        excluded_schemas = self.param_exclude_schemas + ['dbo']

        try:
            r = self.param_sql_session.execute("""
    SELECT SCHEMA_NAME
    FROM INFORMATION_SCHEMA.SCHEMATA s
    WHERE exists(SELECT 1
                 FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = s.SCHEMA_NAME)
    ORDER BY SCHEMA_NAME
            """)
        except Exception as e:
            print(e)
            raise SystemExit('Error connecting to database {}'.format(e.orig))

        result = []
        for row in r:
            if row['SCHEMA_NAME'] not in excluded_schemas:
                result.append(self.translate_a_name(row['SCHEMA_NAME']))

        return result

    def read_tables(self):
        r = self.param_sql_session.execute("""
SELECT TABLE_SCHEMA, TABLE_NAME
FROM information_schema.tables
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME NOT IN ('dtproperties', 'sysdiagrams')
ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)

        result = []
        for row in r:
            if row["TABLE_SCHEMA"] not in self.param_exclude_schemas:
                translated_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])

                if row["TABLE_SCHEMA"] == 'dbo':
                    original_name = '[{}]'.format(row["TABLE_NAME"])
                else:
                    original_name = '[{}].[{}]'.format(row["TABLE_SCHEMA"], row["TABLE_NAME"])

                result.append({
                    'original_name': original_name,
                    'translated_name': translated_name,
                })

        return result

    def read_columns(self):
        r = self.param_sql_session.execute("""
SELECT TABLE_SCHEMA,
  TABLE_NAME,
  COLUMN_NAME,
  COLUMN_DEFAULT,
  IS_NULLABLE,
  DATA_TYPE,
  CHARACTER_MAXIMUM_LENGTH,
  NUMERIC_PRECISION,
  NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)

        result = {}
        for row in r:
            table_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])
            table_column = dict(
                name=row['COLUMN_NAME'],
                type=row['DATA_TYPE'],
                default=row['COLUMN_DEFAULT'],
                nullable=row['IS_NULLABLE'],
                char_length=row['CHARACTER_MAXIMUM_LENGTH'],
                precision=row['NUMERIC_PRECISION'],
                scale=row['NUMERIC_SCALE'],
            )

            table_column['translated_name'] = self.translate_a_name(row['COLUMN_NAME'])
            table_column['translated_type'] = self.translate_column_type(table_column)
            table_column['translated_default'] = self.translate_default(table_column['translated_type'], table_column['default'])

            if table_name not in result:
                result[table_name] = []

            result[table_name].append(table_column)

        return result

    def read_computed_columns(self, columns):
        r = self.param_sql_session.execute("""
SELECT S.NAME TABLE_SCHEMA, T.NAME TABLE_NAME, C.NAME COLUMN_NAME, C.DEFINITION
FROM SYS.COMPUTED_COLUMNS C
INNER JOIN SYS.TABLES T
  ON T.OBJECT_ID = C.OBJECT_ID
INNER JOIN SYS.SCHEMAS S
  ON S.SCHEMA_ID = T.SCHEMA_ID
        """)

        for row in r:
            table_columns = columns[self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])]
            for column in table_columns:
                if column['name'] == row['COLUMN_NAME']:
                    column['computed'] = row['DEFINITION']

    def read_constraints_pk_uk(self):
        r = self.param_sql_session.execute("""
SELECT u.TABLE_SCHEMA, u.TABLE_NAME, u.COLUMN_NAME, c.CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE u
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
  ON  c.CONSTRAINT_NAME = u.CONSTRAINT_NAME
  AND c.CONSTRAINT_SCHEMA = u.CONSTRAINT_SCHEMA
WHERE c.CONSTRAINT_TYPE IN ('UNIQUE', 'PRIMARY KEY')
        """)

        result = []
        for row in r:
            if row['TABLE_SCHEMA'] not in self.param_exclude_schemas:
                table_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])
                pk = {
                    'type': row['CONSTRAINT_TYPE'],
                    'table': table_name,
                    'column': self.translate_a_name(row['COLUMN_NAME']),
                }
                result.append(pk)

        return result

    def read_constraints_check(self):
        r = self.param_sql_session.execute("""
SELECT c.TABLE_SCHEMA, c.TABLE_NAME, h.CHECK_CLAUSE
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS h
  ON  c.CONSTRAINT_NAME = h.CONSTRAINT_NAME
  and c.CONSTRAINT_SCHEMA = h.CONSTRAINT_SCHEMA
WHERE CONSTRAINT_TYPE = 'CHECK'
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME
        """)

        result = []
        for row in r:
            if row['TABLE_SCHEMA'] not in self.param_exclude_schemas:
                table_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])
                pk = {
                    'table': table_name,
                    'clause': self.translate_check_constraint(row['CHECK_CLAUSE']),
                }
                result.append(pk)

        return result

    def read_constraints_fk(self):
        r = self.param_sql_session.execute("""
SELECT KCU1.CONSTRAINT_SCHEMA AS CONSTRAINT_SCHEMA,
  KCU1.CONSTRAINT_NAME AS CONSTRAINT_NAME,
  KCU1.TABLE_SCHEMA AS TABLE_SCHEMA,
  KCU1.TABLE_NAME AS TABLE_NAME,
  KCU1.COLUMN_NAME AS COLUMN_NAME,
  KCU1.ORDINAL_POSITION AS ORDINAL_POSITION,
  KCU2.CONSTRAINT_SCHEMA AS UNIQUE_CONSTRAINT_SCHEMA,
  KCU2.CONSTRAINT_NAME AS UNIQUE_CONSTRAINT_NAME,
  KCU2.TABLE_SCHEMA AS UNIQUE_TABLE_SCHEMA,
  KCU2.TABLE_NAME AS UNIQUE_TABLE_NAME,
  KCU2.COLUMN_NAME AS UNIQUE_COLUMN_NAME
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
  ON  KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
  AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
  AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
  ON  KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
  AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
  AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
WHERE KCU1.ORDINAL_POSITION = KCU2.ORDINAL_POSITION
  AND KCU1.TABLE_SCHEMA not in ('sys', 'guest', 'information_schema', 'elms', 'rms', 'rms_old')
ORDER BY CONSTRAINT_SCHEMA, CONSTRAINT_NAME
        """)

        result = []
        for row in r:
            table_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])
            pk_table_name = self.translate_table_name(row["UNIQUE_TABLE_SCHEMA"], row["UNIQUE_TABLE_NAME"])
            pk = {
                'table': table_name,
                'column': self.translate_a_name(row['COLUMN_NAME']),
                'pk_table': pk_table_name,
                'pk_column': self.translate_a_name(row['UNIQUE_COLUMN_NAME']),
            }
            result.append(pk)

        return result

    def read_indexes(self):
        r = self.param_sql_session.execute("""
SELECT sch.name as TABLE_SCHEMA,
     t.name as TABLE_NAME,
     ind.name as INDEX_NAME,
     ind.index_id as INDEX_ID,
     ic.index_column_id as COLUMN_ID,
     col.name as COLUMN_NAME
FROM sys.indexes ind
INNER JOIN sys.index_columns ic
  ON  ind.object_id = ic.object_id
  AND ind.index_id = ic.index_id
INNER JOIN sys.columns col
  ON ic.object_id = col.object_id
  AND ic.column_id = col.column_id
INNER JOIN sys.tables t
  ON ind.object_id = t.object_id
INNER JOIN sys.schemas sch
  ON sch.schema_id = t.schema_id
WHERE ind.is_primary_key = 0
  AND ind.is_unique = 0
  AND ind.is_unique_constraint = 0
  AND t.is_ms_shipped = 0
  AND sch.name not in ('sys', 'guest', 'information_schema', 'elms', 'rms', 'rms_old')
ORDER BY t.name, ind.name, ind.index_id, ic.index_column_id
        """)

        new_indexes = {}
        result = []
        index = {}
        for row in r:
            if row['TABLE_SCHEMA'] not in self.param_exclude_schemas:
                if row["COLUMN_ID"] == 1:
                    if len(index.keys()) > 0:
                        result.append(index)

                    table_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])
                    index_name = self.translate_a_name(row['TABLE_NAME'])
                    if index_name.startswith('"'):
                        index_name = '"index_{}'.format(index_name[1:])
                    else:
                        index_name = 'index_{}'.format(index_name)


                    if index_name in new_indexes:
                        if new_indexes[index_name] == 0:
                            new_indexes[index_name] = 2
                        else:
                            new_indexes[index_name] += 1

                        index_name = '{}{}'.format(index_name, new_indexes[index_name])
                    else:
                        index_id = 0
                        new_indexes[index_name] = 0

                    index = dict(
                        table_name=table_name,
                        index_name=index_name,
                        columns=[],
                    )

                index['columns'].append(self.translate_a_name(row['COLUMN_NAME']))

        return result

    def read_identity_columns(self):
        r = self.param_sql_session.execute("""
SELECT s.name TABLE_SCHEMA,
    o.name TABLE_NAME,
    c.name COLUMN_NAME
FROM sys.identity_columns c
    INNER JOIN sys.objects o
        ON o.object_id = c.object_id
    INNER JOIN sys.schemas s
        ON o.schema_id = s.schema_id
WHERE s.name NOT IN ('sys')
ORDER BY 1, 2, 3
        """)

        result = {}
        for row in r:
            if row['TABLE_SCHEMA'] not in self.param_exclude_schemas:
                table_name = self.translate_table_name(row["TABLE_SCHEMA"], row["TABLE_NAME"])

                if table_name.endswith('"'):
                    sequence_name = '{}_seq"'.format(table_name[0:len(table_name)-1])
                else:
                    sequence_name = '{}_seq'.format(table_name)

                sequence = dict(
                    original_column_name=row["COLUMN_NAME"],
                    column_name=self.translate_a_name(row["COLUMN_NAME"]),
                    max_value=0,
                    sequence_name=sequence_name,
                )

                result[table_name] = sequence

        return result

    ###########################################################################
    # Output

    def write_string(self, s):
        if self.param_output_file is not None:
            self.param_output_file.write(s + '\n')
        else:
            print(s)

    def output_progress(self, comment):
        if self.param_output_file is not None:
            print(comment)

    def progress_at_10_percent(self, current, total):
        result = 0
        for percent in (10, 20, 30, 40, 50, 60, 70, 80, 90):
            if current*100/total <= percent and (current+1)*100/total > percent:
                result = percent

        return result

    def output_section(self, comment):
        self.write_string('\n--')
        if comment is not None:
            self.write_string('-- {}'.format(comment))
        else:
            self.write_string('--')

    def output_database(self):
        self.output_section('PREPARE DATABASE')

        self.write_string("""
\connect postgres
drop database {db};
create database {db};
\connect {db}

CREATE EXTENSION "uuid-ossp";
        """.format(db=self.param_destination_database))

    def output_schemas(self):
        if len(self.schemas) > 0:
            self.output_section('CREATE SCHEMAS')

        for schema in self.schemas:
            self.write_string('CREATE SCHEMA {};'.format(schema))

    def output_table_columns(self, table_name, table_columns):
        if table_name in self.sequences:
            sequence_column = self.sequences[table_name]['column_name']
            sequence_name = self.sequences[table_name]['sequence_name']
        else:
            sequence_column = None

        count = 0
        for column in table_columns:
            if column['nullable'] == 'NO':
                nullable = 'NOT NULL'
            else:
                nullable = ''

            if column['translated_default'] is None:
                if sequence_column == column['translated_name']:
                    default_constraint = "DEFAULT nextval('{}')".format(sequence_name)
                else:
                    default_constraint = ''
            else:
                default_constraint = 'DEFAULT {}'.format(column['translated_default'])

            column_definition = '{name} {type} {nullable} {default}{computed}'.format(
                name=column['translated_name'],
                type=column['translated_type'],
                nullable=nullable,
                default=default_constraint,
                computed='' if 'computed' not in column else ' /* computed column: {}*/'.format(column['computed']),
            )

            column_definition = '    ' + column_definition.strip()

            count += 1
            if count < len(table_columns):
                column_definition += ','

            self.write_string(column_definition)

    def output_tables(self):
        if len(self.tables) > 0:
            self.output_section('CREATE TABLES')

        for table in self.tables:
            table_name = table['translated_name']
            table_columns = self.columns[table_name]

            self.write_string('CREATE TABLE {} ('.format(table_name))
            self.output_table_columns(table_name, table_columns)
            self.write_string(');')

            pk = [x['column'] for x in self.constraints_pk_uk if x['table'] == table_name and x['type'] == 'PRIMARY KEY']
            if len(pk) > 0:
                self.write_string('ALTER TABLE {} ADD PRIMARY KEY ({});'.format(table_name, ', '.join(pk)))

            uk = [x['column'] for x in self.constraints_pk_uk if x['table'] == table_name and x['type'] == 'UNIQUE']
            if len(uk) > 0:
                self.write_string('ALTER TABLE {} ADD UNIQUE ({});'.format(table_name, ', '.join(uk)))

            check = [x['clause'] for x in self.constraints_check if x['table'] == table_name]
            if len(check) > 0:
                self.write_string('-- ALTER TABLE {} ADD CHECK {};'.format(table_name, ', '.join(check)))

            self.write_string('')

    def output_data(self):
        if len(self.tables) > 0:
            self.output_section('INSERT DATA')

        table_count = 0
        for table in self.tables:
            table_count += 1
            # Output progress every 10% approximately
            percentage = self.progress_at_10_percent(table_count, len(self.tables))
            if percentage != 0:
                self.output_progress('    {}%'.format(percentage))

            table_columns = self.columns[table['translated_name']]

            if table['translated_name'] in self.sequences:
                sequence = self.sequences[table['translated_name']]
            else:
                sequence = None

            r = self.param_sql_session.execute("SELECT * FROM {}".format(table['original_name']))

            row_count = 0
            header_printed = False
            for row in r:
                if not header_printed:
                    self.write_string('\echo')
                    self.write_string('\echo Importing table [{}]'.format(table['translated_name']))
                    self.write_string('\echo')

                    column_string = ', '.join([column['translated_name'] for column in table_columns])
                    self.write_string('COPY {} ({}) FROM stdin;'.format(table['translated_name'], column_string))
                    header_printed = True

                if sequence is not None:
                    sequence['max_value'] = max(sequence['max_value'], row[sequence['original_column_name']])

                row_data = []
                for column in table_columns:
                    cell = self.translate_data(row[column['name']], column['translated_type'])
                    row_data.append(cell)

                self.write_string('\t'.join(row_data))

                row_count += 1
                if row_count >= self.param_max_record_count:
                    break

            if header_printed:
                self.write_string('\\.\n\n')

    def output_fk_constraints(self):
        if len(self.constraints_fk) > 0:
            self.output_section('CREATE REFERENTIAL CONSTRAINTS')

        for constraint in self.constraints_fk:
            self.write_string('ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}({});'.format(
                constraint['table'],
                constraint['column'],
                constraint['pk_table'],
                constraint['pk_column'],
            ))

    def output_indexes(self):
        if len(self.indexes) > 0:
            self.output_section('CREATING INDEXES')

        for index in self.indexes:
            index_columns = ', '.join(index['columns'])
            index_definition = 'CREATE INDEX {} on {}({});'.format(index['index_name'], index['table_name'], index_columns)

            self.write_string(index_definition)

    def output_sequences(self):
        if len(self.sequences) > 0:
            self.output_section('CREATING SEQUENCES')

        for sequence in self.sequences:
            sequence_definition = 'CREATE SEQUENCE {};'.format(self.sequences[sequence]['sequence_name'])

            self.write_string(sequence_definition)

    def output_sequences_start_values(self):
        if len(self.sequences) > 0:
            self.output_section('UPDATING SEQUENCE START VALUES')

        for sequence in self.sequences:
            s = self.sequences[sequence]
            sequence_definition = 'ALTER SEQUENCE {} START WITH {};'.format(self.sequences[sequence]['sequence_name'], s['max_value']+1)

            self.write_string(sequence_definition)

converter = MsSql2Pg()
converter.run()
