package it.sineo.ttrssMySQL2PGSQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

public class Sync {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Class.forName("org.postgresql.Driver");
		Class.forName("com.mysql.jdbc.Driver");

		final String srcUrl = System.getProperty("src.url", "jdbc:mysql://localhost/ttrssdb");
		final String dstUrl = System.getProperty("dst.url", "jdbc:postgresql:ttrssdb");

		final String user = System.getProperty("username");
		final String password = System.getProperty("password");

		if (srcUrl == null || dstUrl == null || user == null || password == null) {
			System.err.println("Missing either src.url, dst.url, username or password. Try again");
			System.exit(-1);
		}

		Connection srcCon = DriverManager.getConnection(srcUrl, user, password);
		Connection dstCon = DriverManager.getConnection(dstUrl, user, password);

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

			String[] tableData = tableInfo.split(":");
			String tableName = tableData[0];
			/* Is it a table w/ a serial column? */
			String idColumn = tableData.length > 1 ? tableData[1] : null;
			long lastIdValue = -1;
			long t0 = System.currentTimeMillis();
			String query = "SELECT * FROM " + tableName;
			if (idColumn != null) {
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
						case Types.LONGVARCHAR: {
							String _str = rs.getString(i);
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
						case Types.VARCHAR: {
							String _str = rs.getString(i);
							wasNull = rs.wasNull();
							if (!wasNull) {
								pst.setString(j, _str);
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
					pst.executeUpdate();
					// pst.addBatch();
					// count++;
					// if (count % 500 == 0) {
					// pst.executeBatch();
					// }
				} catch (SQLException sqlex) {
					System.out.println("ERROR: " + sqlex.getMessage());
				}
			}
			// pst.executeBatch();

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
