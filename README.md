# cloudfs_fdw

A foreign data wrapper for accessing CSV, JSON, EXCEL and ODF files on cloud filesystems.

## Installation

`CREATE EXTENSION multicorn;`

`CREATE SERVER <server> FOREIGN DATA WRAPPER multicorn OPTIONS (wrapper 'cloudfs_fdw.cloudfs_fdw.cloudfs_fdw');`

`CREATE USER MAPPING FOR <user> SERVER <server>;`

### Create table

`CREATE FOREIGN TABLE <schema>.<table> (...) SERVER <server> OPTIONS (<options>);`

See [this](https://www.postgresql.org/docs/11/ddl-foreign-data.html) for general information how SQL/MED in PostgreSQL works, and [this](https://www.postgresql.org/docs/11/sql-createforeigntable.html) for CREATE FOREIGN TABLE syntax.

### Available Options

#### S3 compatible

* source 's3'
* bucket &lt;bucket&gt;
* filepath &lt;filename&gt;
* host &lt;host&gt;, default: localhost
* port &lt;port&gt;, default: 443
* region &lt;region&gt;
* aws_access_key &lt;access_key&gt;
* aws_secret_key &lt;secret_key&gt;

#### HTTP / HTTPS

* source 'http/https'
* url &lt;URL&gt;

#### Filesystem

* source 'file'
* filepath &lt;filepath&gt;&gt;

#### JSON

* format 'json'
* json_path &lt;json_path&gt;

#### CSV

* format 'csv'
* header &lt;true/false&gt;
* delimiter &lt;delimiter&gt;, default: ,
* quote_char &lt;quote_char&gt;, default: "

#### EXCEL

 * format 'excel'
 * header &lt;true/false&gt;
 * sheet &lt;sheet_name&gt;, default: first sheet

#### ODF

 * format 'odf'
 * header &lt;true/false&gt;
 * sheet &lt;sheet_name&gt;, default: first sheet

#### Compression

'.gz' and '.zip' compressed files are decompressed automagically.
