# TT-RSS MySQL-to-PostgreSQL migration tool
This is a simple tool to migrate an existing [TT-RSS] installation from a MySQL database to a PostgreSQL one.

## Usage

### Prerequisites

1. MySQL (original) up and running. Make sure your database schema is up to date to the running TT-RSS instance.
2. PostgreSQL up and running. Create the database, the user, give the required grants (example: `createdb ttrssdb -O ttrssuser -D ttrss` as postgres user, but I recommend looking at the PostgreSQL docs for more information).
3. Execute the ttrss_schema_pgsql.sql script on the new database, using the file coming from your running TT-RSS instance (so it's up-to-date to your MySQL instance)
4. Do not configure TT-RSS to connect to PostrgreSQL yet. And don't perform any login as any user.

### Launching the tool

1. Edit the config.properties file with your own database configuration. It's likely you'll only need to change the database name, username and password.
2. Stop the TT-RSS update daemon if it is running.
3. "java -jar tt-rss-mysql2pgsql.jar". You may be required to add -XmxNNNNm if you have a big database and set usebatch=true in config.properties. The exact amount of RAM to allocate depends on your database size.
4. Edit the config.php in your TT-RSS installation in order to use pgsql driver in place of mysql.

## Migration notes

1. All existing users will be copied EXCEPT for admin. All his preferences will be
migrated, EXCEPT for his password. He'll have the original one ("password" as of the latest scripts)
2. All feeds will be copied EXCEPT for TT-RSS Forum and Releases (which are
created by the ttrss_schema_pgsql.sql you run at step 0 and don't require migration)

## Compatibility

This is a quick & dirty tool, even writing this doc seems going too far. It has been originally designed for on 1.7.9/1.8 in June 2013.
Since the tables are written in the code (quick & dirty tool, remember?), I'll update it should Fox add new tables to the script. As of 2017-06-03 there hasn't been a new table in the past four years (2013-06-13) so I guess the tool is pretty stable.
Last verified schema version: 130

[TT-RSS]: http://tt-rss.org
