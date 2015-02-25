# mssql2pg
Database conversion script from MS SQL Server to PostgreSQL.
 * The script works from command line.
 * Runs in python 2.x or 3.x
 * Produces a SQL script similar to PostgreSQL default backup to create database, all objects and import data. 
	 * Data import is done via quick ```COPY``` command, not individual inserts
	 * Just use ```psql -f FILE_NAME``` to create a database after script ran.
	 * Converts “dbo” schema objects to “public”, no “dbo” schema will be created in PostgreSQL
	 * Example of such a script is below.
 * Converts types from SQL Server to PostgreSQL
 * Generates sequences for ```IDENTITY``` fields
 * Scripts outputs progress, so you don’t need to guess if it’s working or froze up.

##Dependencies
 * FreeTDS
 * pymssql
 * sqlalchemy

##Automatically Generated Usage Message
```
usage: mssql2pg.py [-h] [-p PASSWORD] [-d DESTINATION_DATABASE]
                   [-f OUTPUT_FILE_NAME] [-u] [-n RECORD_COUNT]
                   [-x EXCLUDE_SCHEMAS]
                   host_name database_name login_name

Convert Microsoft SQL Server database into PostgreSQL. Produces .sql script
that can be executed with psql.

positional arguments:
  host_name             SQL Server host name
  database_name         Source database name
  login_name            Login name

optional arguments:
  -h, --help            show this help message and exit
  -p PASSWORD, --password PASSWORD
                        Password for the login_name
  -d DESTINATION_DATABASE, --destination-database DESTINATION_DATABASE
                        If not provided, destination database name will be the
                        same as source.
  -f OUTPUT_FILE_NAME, --file OUTPUT_FILE_NAME
                        If not provided, script will be printed to standard
                        output.
  -u, --underscore      Convert CamelCase into underscored_identifiers
                        (schemas, tables and columns).
  -n RECORD_COUNT, --limit_records RECORD_COUNT
                        For test runs, process only provided number of records
                        per table. WARNING: foreign keys may not import
                        properly.
  -x EXCLUDE_SCHEMAS, --exclude-schemas EXCLUDE_SCHEMAS
                        Comma separated (no spaces) list of schemas that will
                        be excluded from export. If not provided, all schemas
                        will be processed.
```

##Example:
```
python3 mssql2pg.py SqlServer PgConversionExample user1 p4ssw0rd -f example.sql -d conversion_example -u
```

##Output:
```
--
-- PREPARE DATABASE

\connect postgres
drop database ubt;
create database ubt;
\connect ubt

CREATE EXTENSION "uuid-ossp";
        
--
-- CREATING SEQUENCES
CREATE SEQUENCE ourfurnature_seq;

--
-- CREATE TABLES
CREATE TABLE ourfurnature (
    id INT NOT NULL DEFAULT nextval('ourfurnature_seq'),
    name VARCHAR(50)
);
ALTER TABLE ourfurnature ADD PRIMARY KEY (id);

--
-- INSERT DATA
\echo
\echo Importing table [ourfurnature]
\echo
COPY ourfurnature (id, name) FROM stdin;
1	table
2	chair
3	bed
\.

--
-- UPDATING SEQUENCE START VALUES
ALTER SEQUENCE ourfurnature_seq START WITH 4;
```