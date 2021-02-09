package com.ric.dividaspgfn;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.fusesource.jansi.Ansi;

final class Common {
	static final Charset CHARSET = StandardCharsets.ISO_8859_1;

	private Common() { }

	public static int executeUpdateStatement(Connection connection, String statement) throws SQLException {
		try (Statement stmt = connection.createStatement();) {
			return stmt.executeUpdate(statement);
		}
	}
	
	public static void mensagemErro(String mensagem) {
		System.out.println(Ansi.ansi().fgBright(Ansi.Color.RED).a(mensagem).reset());
	}
	
	public static void mensagemProgresso(String mensagem) {
		System.out.println(Ansi.ansi().fgBright(Ansi.Color.GREEN).a(mensagem).reset());
	}
}
