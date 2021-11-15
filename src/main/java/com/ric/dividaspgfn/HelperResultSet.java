package com.ric.dividaspgfn;

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

final class HelperResultSet extends ResultSetHelperService {
	private static final String DEFAULT_VALUE = "";

	private DecimalFormat numberFormat = new DecimalFormat("#.###", DecimalFormatSymbols.getInstance(Locale.forLanguageTag("pt-BR")));

	@Override
	public String[] getColumnValues(ResultSet rs, boolean trim, String dateFormatString, String timeFormatString) throws SQLException, IOException {
		ResultSetMetaData metadata = rs.getMetaData();
		String[] valueArray = new String[metadata.getColumnCount()];
		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			valueArray[i-1] = getColumnValue(rs, metadata.getColumnType(i), i, trim, dateFormatString, timeFormatString, this.numberFormat);
		}
		return valueArray;
	}

	private String getColumnValue(ResultSet rs, int colType, int colIndex, boolean trim, String dateFormatString, String timestampFormatString, DecimalFormat decimalFormat) throws SQLException, IOException {
		String value;

		switch (colType) {
		case Types.BOOLEAN:
			value = Objects.toString(rs.getBoolean(colIndex));
			break;
		case Types.NCLOB:
			value = handleNClob(rs, colIndex);
			break;
		case Types.CLOB:
			value = handleClob(rs, colIndex);
			break;
		case Types.BIGINT: {
			BigDecimal d = rs.getBigDecimal(colIndex);
			value = Objects.toString(d!=null?d.toBigInteger():null);
			break;
		}
		case Types.DECIMAL:
		case Types.REAL:
		case Types.NUMERIC: {
			BigDecimal d = rs.getBigDecimal(colIndex);
			value = d!=null?decimalFormat.format(d):null;
			break;
		}
		case Types.DOUBLE:
			value = decimalFormat.format(rs.getDouble(colIndex));
			break;
		case Types.FLOAT:
			value = decimalFormat.format(rs.getFloat(colIndex));
			break;
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
			value = Objects.toString(rs.getInt(colIndex));
			break;
		case Types.DATE:
			value = handleDate(rs, colIndex, dateFormatString);
			break;
		case Types.TIME:
			value = Objects.toString(rs.getTime(colIndex), DEFAULT_VALUE);
			break;
		case Types.TIMESTAMP:
			value = handleTimestamp(rs.getTimestamp(colIndex), timestampFormatString);
			break;
		case Types.NVARCHAR:
		case Types.NCHAR:
		case Types.LONGNVARCHAR:
			value = handleNVarChar(rs, colIndex, trim);
			break;
		case Types.LONGVARCHAR:
		case Types.VARCHAR:
		case Types.CHAR:
			value = handleVarChar(rs, colIndex, trim);
			break;
		default:
			// This takes care of Types.BIT, Types.JAVA_OBJECT, and anything
			// unknown.
			value = Objects.toString(rs.getObject(colIndex), DEFAULT_VALUE);
		}

		if (rs.wasNull() || value == null) {
			value = DEFAULT_VALUE;
		}

		return value;
	}
}
