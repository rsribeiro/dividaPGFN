module com.ric.dividapgfn {
	requires java.sql;
	requires info.picocli;
	requires com.opencsv;
	requires org.tinylog.api;
	requires org.xerial.sqlitejdbc;

	opens com.ric.dividapgfn to info.picocli;
}