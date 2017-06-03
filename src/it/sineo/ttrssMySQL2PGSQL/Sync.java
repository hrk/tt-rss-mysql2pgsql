package it.sineo.ttrssMySQL2PGSQL;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

public class Sync {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Class.forName("org.postgresql.Driver");
		Class.forName("com.mysql.jdbc.Driver");

		final Properties config = new Properties();
		try {
			InputStream is = Sync.class.getResourceAsStream("config");
			if (is == null) {
				is = new FileInputStream("config.properties");
			}
			config.load(is);
			is.close();
		} catch (IOException ioex) {
			System.err.println("Unable to read config.properties due to exception: " + ioex.getMessage());
		}

		String srcUrl = config.getProperty("mysql.url", null);
		String dstUrl = config.getProperty("pgsql.url", null);

		String srcUser = config.getProperty("mysql.username", null);
		String srcPassword = config.getProperty("mysql.password", null);

		String dstUser = config.getProperty("pgsql.username", null);
		String dstPassword = config.getProperty("pgsql.password", null);

		final boolean useBatch = Boolean.valueOf(config.getProperty("usebatch", "true"));

		if (dstUser == null) {
			dstUser = srcUser;
		}
		if (dstPassword == null) {
			dstPassword = srcPassword;
		}

		if (srcUrl == null || dstUrl == null || srcUser == null || srcPassword == null) {
			System.err.println("Check your config.properties, something is missing!");
			System.exit(-1);
		}

		Connection srcCon = DriverManager.getConnection(srcUrl, srcUser, srcPassword);
		Connection dstCon = DriverManager.getConnection(dstUrl, dstUser, dstPassword);

		String[] tableList = {
				"ttrss_users:id",
				"ttrss_feed_categories:id",
				"ttrss_feeds:id",
				"ttrss_archived_feeds",
				"ttrss_counters_cache",
				"ttrss_cat_counters_cache",
				"ttrss_entries:id",
				"ttrss_user_entries:int_id",
				"ttrss_entry_comments:id",
				"ttrss_filters2:id",
				"ttrss_filters2_rules:id",
				"ttrss_filters2_actions:id",
				"ttrss_tags:id",
				"ttrss_enclosures:id",
				"ttrss_settings_profiles:id",
				"ttrss_user_prefs",
				"ttrss_feedbrowser_cache",
				"ttrss_labels2:id",
				"ttrss_user_labels2",
				"ttrss_access_keys:id",
				"ttrss_linked_instances:id",
				"ttrss_linked_feeds",
				"ttrss_plugin_storage:id",
				"ttrss_error_log:id"
		};

		Statement srcSt = srcCon.createStatement();
		PreparedStatement dstSequence = dstCon.prepareStatement("SELECT setval(?, ?)");
		for (String tableInfo : tableList) {
			int count = 0;

			String[] tableData = tableInfo.split(":");
			String tableName = tableData[0];
			/* Is it a table w/ a serial column? */
			String idColumn = tableData.length > 1 ? tableData[1] : null;
			long lastIdValue = -1;
			long t0 = System.currentTimeMillis();
			String query = "SELECT * FROM " + tableName;
			if (idColumn != null) {
				/*
				 * We assume the serial never overflows and starts again filling the
				 * holes...
				 */
				query += " ORDER BY " + idColumn + " ASC";
			}
			ResultSet rs = srcSt.executeQuery(query);

			String insertHead = "INSERT INTO " + tableName + "(";
			String insertTail = ") VALUES (";
			ResultSetMetaData rsmd = rs.getMetaData();
			final int start = /* tableData.length > 1 ? 2 : */1;
			for (int i = start; i <= rsmd.getColumnCount(); i++) {
				if (i > start) {
					insertHead += ", ";
					insertTail += ", ";
				}
				insertHead += rsmd.getColumnName(i);
				insertTail += "?";
			}
			String insert = insertHead + insertTail + ")";
			System.out.println(insert);
			PreparedStatement pst = dstCon.prepareStatement(insert);
			while (rs.next()) {
				/*
				 * The default script creates two rows: the admin user and the feed for
				 * the forum. In order to avoid exceptions on those two rows, we'll skip
				 * them.
				 */
				boolean isOneOfTheTwoAlreadyExistingRows = false;
				for (int i = start; i <= rsmd.getColumnCount(); i++) {
					int j = /* tableData.length > 1 ? i - 1 : */i;
					boolean wasNull = false;
					switch (rsmd.getColumnType(i)) {
						case Types.INTEGER: {
							Integer _int = rs.getInt(i);
							wasNull = rs.wasNull();
							if (!wasNull) {
								pst.setInt(j, _int);
								/*
								 * If this is the id column, store its value as we'll need to
								 * update the sequence.
								 */
								if (rsmd.getColumnName(i).equals(idColumn) && _int > lastIdValue) {
									lastIdValue = _int;
								}
							}
							break;
						}
						case Types.TIMESTAMP: {
							Timestamp _ts = rs.getTimestamp(i);
							wasNull = rs.wasNull();
							if (!wasNull) {
								pst.setTimestamp(j, _ts);
							}
							break;
						}
						case Types.LONGVARCHAR:
						case Types.VARCHAR: {
							String _str = rs.getString(i);
							if (("ttrss_users".equals(tableName) && "login".equals(rsmd.getColumnName(i))
									&& "admin".equals(_str))
									|| ("ttrss_feeds".equals(tableName) && "feed_url".equals(rsmd.getColumnName(i))
											&& "http://tt-rss.org/forum/rss.php".equals(_str))) {
								isOneOfTheTwoAlreadyExistingRows = true;
							}
							wasNull = rs.wasNull();
							if (!wasNull) {
								pst.setString(j, _str);
							}
							break;
						}
						case Types.BIT: {
							boolean _b = rs.getBoolean(i);
							wasNull = rs.wasNull();
							if (!wasNull) {
								pst.setBoolean(j, _b);
							}
							break;
						}
						default: {
							System.out.println("Unhandled type: " + rsmd.getColumnTypeName(i) + " " + rsmd.getColumnType(i));
							break;
						}
					} // end-switch.
					if (wasNull) {
						pst.setNull(j, rsmd.getColumnType(i));
					}
				} // end-for: colonne.
				try {
					if (isOneOfTheTwoAlreadyExistingRows) {
						/*
						 * It's either the admin account in ttrss_users or the release feed
						 * in ttrss_feeds; continue to the next row
						 */
						isOneOfTheTwoAlreadyExistingRows = false;
						continue;
					}
					if (useBatch) {
						pst.addBatch();
						count++;
						if (count % 500 == 0) {
							pst.executeBatch();
						}
					} else {
						/* Single row action */
						pst.executeUpdate();
					}
				} catch (SQLException sqlex) {
					System.out.println("ERROR: " + sqlex.getMessage());
					SQLException e = sqlex;
					while (e.getNextException() != null) {
						System.out.println("CAUSE: " + e.getNextException().getMessage());
						e = e.getNextException();
					}
				}
			}
			if (useBatch) {
				/* Final execution */
				try {
					pst.executeBatch();
				} catch (SQLException sqlex) {
					System.out.println("ERROR: " + sqlex.getMessage());
					SQLException e = sqlex;
					while (e.getNextException() != null) {
						System.out.println("CAUSE: " + e.getNextException().getMessage());
						e = e.getNextException();
					}
				}
			}

			long t1 = System.currentTimeMillis();
			System.out.println("[" + tableName + "] " + (t1 - t0) + " ms.");
			/* Update the sequence */
			if (idColumn != null && lastIdValue != -1) {
				System.out.println("[" + tableName + "] setting sequence to " + lastIdValue);
				dstSequence.setString(1, tableName + "_" + idColumn + "_seq");
				dstSequence.setLong(2, lastIdValue);

				dstSequence.execute();
			}

		}
		// End.
		srcCon.close();
		dstCon.close();
	}
}
