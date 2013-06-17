This is a simple tool to migrate a http://tt-rss.org installation from a MySQL database to a PostgreSQL one.

Usage

0. Make sure you already created a PostgreSQL database and launched the ttrss_schema_pgsql.sql script.
1. Edit config.properties with your own configuration. Iit's likely you'll only need to change the database name, username and password.
2. Stop the tt-rss update daemon if it is running.
3. "java -jar tt-rss-mysql2pgsql.jar"
4. Edit your config.php in order to use pgsql driver in place of mysql.

Special notes

1. All users will be copied EXCEPT for admin. All his preferences will be
migrated, EXCEPT for his password. He'll have the original one ("password" as of the latest scripts in 1.8)
2. All feeds will be copied EXCEPT for TT-RSS Forum and Releases (which are
created by the script)

Compatibility

This is a quick & dirty tool, even writing this doc seems going too far. It has been tested on 1.7.9 and 1.8. Since the tables are written in the code, I'll update should Fox add new tables to the script.
