package com.ric.dividaspgfn;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "dividapgfn", subcommands = { BasePGFN.class, FiltraDivida.class })
final class DividaPGFN {
	public static void main(String... args) {
		System.exit(new CommandLine(new DividaPGFN()).execute(args));
	}
}
