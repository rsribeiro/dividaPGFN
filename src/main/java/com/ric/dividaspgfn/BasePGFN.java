package com.ric.dividaspgfn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Stream;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "base_pgfn", aliases = "base", description = "Cria base em formato SQLite a partir dos arquivos disponibilizados pela PGFN")
final class BasePGFN implements Callable<Integer> {
	@Option(names = { "-d", "--dir" }, required = true, description = "Diretório base")
	private Path dirBase;

	@Option(names = { "-s", "--batchsize" }, description = "Quantidade de linhas inseridas no banco de dados por batelada, possível alterar em caso de baixo desempenho")
	private int batchSize = 1000;

	@Override
	public Integer call() throws Exception {
		Path dirEntrada = this.dirBase.resolve("entrada");

		Path dirFGTS = dirEntrada.resolve("FGTS");
		Path dirPrevidenciaria = dirEntrada.resolve("Previdenciario");
		Path dirGeral = dirEntrada.resolve("Nao_Previdenciario");
		Path baseConsolidada = dirEntrada.resolve("pgfn.sqlite");

		Files.deleteIfExists(baseConsolidada);

		Common.mensagemProgresso("Criando base em " + baseConsolidada.toAbsolutePath());

		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + baseConsolidada.toAbsolutePath());) {
			Common.executeUpdateStatement(conn, "CREATE TABLE pgfn_devedores " +
					"(`cpf_cnpj` TEXT, " +
					"`tipo_pessoa` TEXT, " +
					"`tipo_devedor` TEXT, " +
					"`nome_devedor` TEXT, " +
					"`uf_unidade_responsavel` TEXT, " +
					"`unidade_responsavel` TEXT, " +
					"`entidade_responsavel` TEXT, " +
					"`unidade_inscricao` TEXT, " +
					"`numero_inscricao` TEXT, " +
					"`tipo_situacao_inscricao` TEXT, " +
					"`situacao_inscricao` TEXT, " +
					"`receita_principal` TEXT, " +
					"`tipo_credito` TEXT, " +
					"`data_inscricao` TEXT, " +
					"`indicador_ajuizado` TEXT, " +
					"`valor_consolidado` REAL, " +
					"`arquivo_origem` TEXT)");

			Stream<LinhaPGFN> linhas = Stream
					.of(leBase(dirFGTS, this::linhaFGTS), leBase(dirPrevidenciaria, this::linhaPrevidenciaria), leBase(dirGeral, this::linhaGeral))
					.flatMap(Function.identity());

			conn.setAutoCommit(false);
			System.out.println("Inserindo linhas...");
			try (PreparedStatement stmt = conn.prepareStatement("insert into pgfn_devedores (cpf_cnpj, " +
					"tipo_pessoa, " +
					"tipo_devedor, " +
					"nome_devedor, " +
					"uf_unidade_responsavel, " +
					"unidade_responsavel, " +
					"entidade_responsavel, " +
					"unidade_inscricao, " +
					"numero_inscricao, " +
					"tipo_situacao_inscricao, " +
					"situacao_inscricao, " +
					"receita_principal, " +
					"tipo_credito, " +
					"data_inscricao, " +
					"indicador_ajuizado, " +
					"valor_consolidado, " +
					"arquivo_origem) " +
					"values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");) {
				BatchAux aux = new BatchAux(stmt);
				linhas.forEach(aux::appendLinha);
				stmt.executeBatch();
			} finally {
				conn.commit();
			}
			System.out.println("Linhas inseridas.");

			System.out.println("Criando índice...");
			Common.executeUpdateStatement(conn, "CREATE INDEX `index_cpf_cnpj` ON `pgfn_devedores` (`cpf_cnpj`)");
			System.out.println("Índice criado.");
		}

		Common.mensagemProgresso("Execução finalizada com sucesso.");

		return 0;
	}

	private Stream<LinhaPGFN> leBase(Path diretorio, Function<String,LinhaPGFN> mapeamento) throws IOException {
		return Files
				.list(diretorio)
				.flatMap((a) -> leArquivoIndividual(a, mapeamento));
	}

	private Stream<LinhaPGFN> leArquivoIndividual(Path entrada, Function<String,LinhaPGFN> mapeamento) {
		try {
			return Files
					.lines(entrada, Common.CHARSET)
					.skip(1)
					.map(mapeamento);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private LinhaPGFN linhaFGTS(String linha) {
		String[] componentes = linha.toUpperCase().split(";");
		if (componentes.length != 15) {
			throw new RuntimeException("Formato do arquivo FGTS diferente do esperado.");
		}

		return LinhaPGFN.linhaFGTS(
				componentes[0],  //CPF_CNPJ
				componentes[1],  //TIPO_PESSOA
				componentes[2],  //TIPO_DEVEDOR
				componentes[3],  //NOME_DEVEDOR
				componentes[4],  //UF_UNIDADE_RESPONSAVEL
				componentes[5],  //UNIDADE_RESPONSAVEL
				componentes[6],  //ENTIDADE_RESPONSAVEL
				componentes[7],  //UNIDADE_INSCRICAO
				componentes[8],  //NUMERO_INSCRICAO
				componentes[9],  //TIPO_SITUACAO_INSCRICAO
				componentes[10], //SITUACAO_INSCRICAO
				componentes[11], //RECEITA_PRINCIPAL
				componentes[12], //DATA_INSCRICAO
				componentes[13], //INDICADOR_AJUIZADO
				componentes[14]  //VALOR_CONSOLIDADO
				);
	}

	private LinhaPGFN linhaPrevidenciaria(String linha) {
		String[] componentes = linha.toUpperCase().split(";");
		if (componentes.length != 13) {
			throw new RuntimeException("Formato do arquivo previdenciário diferente do esperado.");
		}

		return LinhaPGFN.linhaPrevidenciaria(
				componentes[0],  //CPF_CNPJ
				componentes[1],  //TIPO_PESSOA
				componentes[2],  //TIPO_DEVEDOR
				componentes[3],  //NOME_DEVEDOR
				componentes[4],  //UF_UNIDADE_RESPONSAVEL
				componentes[5],  //UNIDADE_RESPONSAVEL
				componentes[6],  //NUMERO_INSCRICAO
				componentes[7],  //TIPO_SITUACAO_INSCRICAO
				componentes[8],  //SITUACAO_INSCRICAO
				componentes[9],  //RECEITA_PRINCIPAL
				componentes[10], //DATA_INSCRICAO
				componentes[11], //INDICADOR_AJUIZADO
				componentes[12]  //VALOR_CONSOLIDADO
				);
	}

	private LinhaPGFN linhaGeral(String linha) {
		String[] componentes = linha.toUpperCase().split(";");
		if (componentes.length != 13) {
			throw new RuntimeException("Formato do arquivo não previdenciário diferente do esperado.");
		}

		return LinhaPGFN.linhaGeral(
				componentes[0],  //CPF_CNPJ
				componentes[1],  //TIPO_PESSOA
				componentes[2],  //TIPO_DEVEDOR
				componentes[3],  //NOME_DEVEDOR
				componentes[4],  //UF_UNIDADE_RESPONSAVEL
				componentes[5],  //UNIDADE_RESPONSAVEL
				componentes[6],  //NUMERO_INSCRICAO
				componentes[7],  //TIPO_SITUACAO_INSCRICAO
				componentes[8],  //SITUACAO_INSCRICAO
				componentes[9],  //TIPO_CREDITO
				componentes[10], //DATA_INSCRICAO
				componentes[11], //INDICADOR_AJUIZADO
				componentes[12]  //VALOR_CONSOLIDADO
				);
	}

	private class BatchAux {
		private final PreparedStatement stmt;
		private int index;

		public BatchAux(PreparedStatement stmt) {
			this.stmt = stmt;
		}

		public void appendLinha(LinhaPGFN linha) {
			try {
				this.stmt.setString(1, linha.cpfCnpj().replace(".", "").replace("/", "").replace("-", "").strip());
				this.stmt.setString(2, linha.tipoPessoa().strip());
				this.stmt.setString(3, linha.tipoDevedor().strip());
				this.stmt.setString(4, linha.nomeDevedor().strip());
				this.stmt.setString(5, linha.ufUnidadeResponsavel().strip());
				this.stmt.setString(6, linha.unidadeResponsavel().strip());
				this.stmt.setString(7, linha.entidadeResponsavel().strip());
				this.stmt.setString(8, linha.unidadeInscricao().strip());
				this.stmt.setString(9, linha.numeroInscricao().strip());
				this.stmt.setString(10, linha.tipoSituacaoInscricao().strip());
				this.stmt.setString(11, linha.situacaoInscricao().strip());
				this.stmt.setString(12, linha.receitaPrincipal().strip());
				this.stmt.setString(13, linha.tipoCredito().strip());
				this.stmt.setString(14, linha.dataInscricao().strip());
				this.stmt.setString(15, linha.indicadorAjuizado().strip());
				this.stmt.setBigDecimal(16, linha.valor());
				this.stmt.setString(17, linha.arquivoOrigem().strip());
				this.stmt.addBatch();

				this.index += 1;
				if (this.index % BasePGFN.this.batchSize == 0) {
					this.stmt.executeBatch();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
