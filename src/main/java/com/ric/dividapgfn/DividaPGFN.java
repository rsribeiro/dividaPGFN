package com.ric.dividapgfn;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "dividapgfn", subcommands = { BasePGFN.class, FiltraDivida.class })
public final class DividaPGFN {
	public static void main(String... args) {
		CommandLine cli = new CommandLine(new DividaPGFN());

		int exitCode = cli.execute(args);
		System.exit(exitCode);
	}
}
