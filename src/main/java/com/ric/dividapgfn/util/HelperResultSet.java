package com.ric.dividapgfn.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Objects;

import com.opencsv.ResultSetHelperService;

public final class HelperResultSet extends ResultSetHelperService {
	private static final String DEFAULT_VALUE = "";

	private DecimalFormat numberFormat = new DecimalFormat("#.###", DecimalFormatSymbols.getInstance(Locale.forLanguageTag("pt-BR")));

	@Override
	public String[] getColumnValues(ResultSet rs, boolean trim, String dateFormatString, String timeFormatString) throws SQLException, IOException {
		ResultSetMetaData metadata = rs.getMetaData();
		String[] valueArray = new String[metadata.getColumnCount()];
		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			valueArray[i-1] = getColumnValue(rs, metadata.getColumnType(i), i, trim, dateFormatString, timeFormatString);
		}
		return valueArray;
	}

	private String getColumnValue(ResultSet rs, int colType, int colIndex, boolean trim, String dateFormatString, String timestampFormatString) throws SQLException, IOException {
		String value = switch (colType) {
		case Types.BOOLEAN -> Objects.toString(rs.getBoolean(colIndex));
		case Types.NCLOB -> handleNClob(rs, colIndex);
		case Types.CLOB -> handleClob(rs, colIndex);
		case Types.BIGINT -> this.handleBigInt(rs, colIndex);
		case Types.DECIMAL, Types.REAL, Types.NUMERIC -> this.handleDecimal(rs, colIndex);
		case Types.DOUBLE -> this.numberFormat.format(rs.getDouble(colIndex));
		case Types.FLOAT -> this.numberFormat.format(rs.getFloat(colIndex));
		case Types.INTEGER, Types.TINYINT, Types.SMALLINT -> Objects.toString(rs.getInt(colIndex));
		case Types.DATE -> handleDate(rs, colIndex, dateFormatString);
		case Types.TIME -> Objects.toString(rs.getTime(colIndex), DEFAULT_VALUE);
		case Types.TIMESTAMP -> handleTimestamp(rs.getTimestamp(colIndex), timestampFormatString);
		case Types.NVARCHAR, Types.NCHAR, Types.LONGNVARCHAR -> handleNVarChar(rs, colIndex, trim);
		case Types.LONGVARCHAR, Types.VARCHAR, Types.CHAR -> handleVarChar(rs, colIndex, trim);
		default -> Objects.toString(rs.getObject(colIndex), DEFAULT_VALUE);
		};

		if (rs.wasNull() || value == null) {
			value = DEFAULT_VALUE;
		}

		return value;
	}

	private String handleBigInt(ResultSet rs, int colIndex) throws SQLException {
		BigDecimal d = rs.getBigDecimal(colIndex);
		return Objects.toString(d != null ? d.toBigInteger() : null);
	}

	private String handleDecimal(ResultSet rs, int colIndex) throws SQLException {
		BigDecimal d = rs.getBigDecimal(colIndex);
		return d != null ? this.numberFormat.format(d) : null;
	}
}
