module com.ric.dividaspgfn {
	requires java.sql;
	requires info.picocli;
	requires org.fusesource.jansi;

	opens com.ric.dividaspgfn to info.picocli;
}