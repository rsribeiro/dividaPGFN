package com.ric.dividapgfn.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import org.sqlite.SQLiteConnection;

public final class Common {
	public static final Charset CHARSET = StandardCharsets.ISO_8859_1;

	private Common() { }

	public static int executeUpdateStatement(SQLiteConnection connection, String statement) throws SQLException {
		try (Statement stmt = connection.createStatement();) {
			return stmt.executeUpdate(statement);
		}
	}

	//Adaptado de https://stackoverflow.com/questions/266825/how-to-format-a-duration-in-java-e-g-format-hmmss
	public static String formatDuration(long nanoseconds) {
		Duration duration = Duration.ofNanos(nanoseconds);

		long minutes = (duration.getSeconds() % 3600) / 60;
		long seconds = duration.getSeconds() % 60;

		return String.format("%dm%02ds", minutes, seconds);
	}
}
