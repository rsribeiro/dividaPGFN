package com.ric.dividapgfn.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;

public class CSVWriter {
	private static final DecimalFormat NUMBER_FORMAT = new DecimalFormat("#.###", DecimalFormatSymbols.getInstance(Locale.of("pt", "BR")));
	private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm:ss", Locale.of("pt", "BR"));
	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.of("pt", "BR"));
	private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.of("pt", "BR"));

	private final char separador;

	public CSVWriter(char separador) {
		this.separador = separador;
	}

	private boolean needsQuote(String str) {
		return str.contains(Character.toString(this.separador))
				|| str.contains("\"")
				|| str.contains("\n")
				|| str.contains("\r");
	}

	private boolean needsEscape(String str) {
		return str.contains("\"");
	}

	private String escape(String str) {
		return str.replace("\"", "\"\"");
	}

	public void write(ResultSet rs, BufferedWriter out, boolean writeHeader) throws SQLException, IOException {
		ResultSetMetaData metadata = rs.getMetaData();
		int colCount = metadata.getColumnCount();

		if (writeHeader) {
			StringJoiner header = new StringJoiner(Character.toString(this.separador));
			for (int col=0; col<colCount; ++col) {
				String str = metadata.getColumnName(col+1);
				if (needsEscape(str)) {
					str = escape(str);
				}
				if (needsQuote(str)) {
					str = new StringBuilder().append('"').append(str).append('"').toString();
				}
				header.add(str);
			}
			out.append(header.toString());
			out.append('\n');
		}

		while (rs.next()) {
			StringJoiner row = new StringJoiner(Character.toString(this.separador));
			for (int col=0; col<colCount; ++col) {
				int colType = metadata.getColumnType(col+1);
				String str = columnToString(rs, col+1, colType);
				if (needsEscape(str)) {
					str = escape(str);
				}
				if (needsQuote(str)) {
					str = new StringBuilder().append('"').append(str).append('"').toString();
				}
				row.add(str);
			}

			out.append(row.toString());
			out.append('\n');
		}
	}

	private String columnToString(ResultSet rs, int col, int colType) throws SQLException {
		String strVal = switch (colType) {
		case Types.BOOLEAN -> Boolean.toString(rs.getBoolean(col));
		case Types.NCLOB, Types.CLOB -> throw new RuntimeException("Unsupported");
		case Types.DECIMAL, Types.REAL, Types.NUMERIC -> handleDecimal(rs, col);
		case Types.BIGINT -> handleBigInt(rs, col);
		case Types.INTEGER, Types.TINYINT, Types.SMALLINT -> Integer.toString(rs.getInt(col));
		case Types.DOUBLE -> NUMBER_FORMAT.format(rs.getDouble(col));
		case Types.FLOAT -> NUMBER_FORMAT.format(rs.getFloat(col));
		case Types.DATE -> handleDate(rs, col);
		case Types.TIME -> handleTime(rs, col);
		case Types.TIMESTAMP -> handleTimestamp(rs, col);
		case Types.NVARCHAR, Types.NCHAR, Types.LONGNVARCHAR -> rs.getNString(col);
		case Types.LONGVARCHAR, Types.VARCHAR, Types.CHAR -> rs.getString(col);
		default -> Objects.toString(rs.getObject(col), "");
		};
		if (rs.wasNull()) {
			return "";
		} else {
			return strVal != null ? strVal.trim() : "";
		}
	}

	private String handleBigInt(ResultSet rs, int col) throws SQLException {
		BigDecimal d = rs.getBigDecimal(col);
		return Objects.toString(d != null ? d.toBigInteger() : null);
	}

	private String handleDecimal(ResultSet rs, int col) throws SQLException {
		BigDecimal d = rs.getBigDecimal(col);
		return d != null ? NUMBER_FORMAT.format(d) : null;
	}

	private String handleDate(ResultSet rs, int col) throws SQLException {
		Date d = rs.getDate(col);
		if (d != null) {
			return d.toLocalDate().format(DATE_FORMAT);
		} else {
			return "";
		}
	}

	private String handleTimestamp(ResultSet rs, int col) throws SQLException {
		Timestamp t = rs.getTimestamp(col);
		if (t != null) {
			return t.toLocalDateTime().format(DATE_TIME_FORMAT);
		} else {
			return "";
		}
	}

	private String handleTime(ResultSet rs, int col) throws SQLException {
		Time t = rs.getTime(col);
		if (t != null) {
			return t.toLocalTime().format(TIME_FORMAT);
		} else {
			return "";
		}
	}
}
