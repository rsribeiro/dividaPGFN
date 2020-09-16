module com.ric.dividaspgfn {
	requires java.sql;
	requires info.picocli;

	opens com.ric.dividaspgfn to info.picocli;
}