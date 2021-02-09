package com.ric.dividaspgfn;

import org.fusesource.jansi.AnsiConsole;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "dividapgfn", subcommands = { BasePGFN.class, FiltraDivida.class })
final class DividaPGFN {
	public static void main(String... args) {
		CommandLine cli = new CommandLine(new DividaPGFN());
		
		boolean jansiInstalled = false;
		try {
			AnsiConsole.systemInstall();
			jansiInstalled = true;
		} catch (Exception e) {
			//ignorar
		}
		int exitCode = cli.execute(args);
		if (jansiInstalled) {
			try {
				AnsiConsole.systemUninstall();
			} catch (Exception e) {
				//ignorar
			}
		}
		System.exit(exitCode);
	}
}
