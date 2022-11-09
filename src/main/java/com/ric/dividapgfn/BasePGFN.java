package com.ric.dividapgfn;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Stream;

import org.tinylog.Logger;

import com.ric.dividapgfn.linha.LinhaFGTS;
import com.ric.dividapgfn.linha.LinhaNaoPrevidenciaria;
import com.ric.dividapgfn.linha.LinhaPrevidenciaria;
import com.ric.dividapgfn.util.Common;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "base_pgfn", aliases = "base", description = "Cria base em formato SQLite a partir dos arquivos disponibilizados pela PGFN")
public final class BasePGFN implements Callable<Integer> {
	@Option(names = { "-d", "--dir" }, required = true, description = "Diretório base")
	private Path dirBase;

	@Option(names = { "-s", "--batchsize" }, description = "Quantidade de linhas inseridas no banco de dados por batelada, possível alterar em caso de baixo desempenho")
	private int batchSize = 1000;

	@Override
	public Integer call() {
		long t0 = System.nanoTime();

		Path dirEntrada = this.dirBase.resolve("entrada");

		Path dirFGTS = dirEntrada.resolve("FGTS");
		Path dirPrevidenciaria = dirEntrada.resolve("Previdenciario");
		Path dirNaoPrevidenciaria = dirEntrada.resolve("Nao_Previdenciario");
		Path baseConsolidada = dirEntrada.resolve("pgfn.sqlite");

		try {
			Files.deleteIfExists(baseConsolidada);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

		Logger.info("Criando base em {}", baseConsolidada.toAbsolutePath());

		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + baseConsolidada.toAbsolutePath());) {
			conn.setAutoCommit(false);

			criaTabelaFGTS(dirFGTS, conn);
			indiceTabelaFGTS(conn);
			criaTabelaPrevidenciaria(dirPrevidenciaria, conn);
			indiceTabelaPrevidenciaria(conn);
			criaTabelaNaoPrevidenciaria(dirNaoPrevidenciaria, conn);
			indiceTabelaNaoPrevidenciaria(conn);
			criaView(conn);
			conn.commit();
		} catch (SQLException e) {
			Logger.error("Erro SQL: {}", e.getLocalizedMessage());
			return CommandLine.ExitCode.SOFTWARE;
		}

		Logger.info("Programa executado em {}", Common.formatDuration(System.nanoTime()-t0));

		return CommandLine.ExitCode.USAGE;
	}

	private void criaView(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando view...");
		Common.executeUpdateStatement(conn, """
				CREATE VIEW pgfn_devedores as
				select
					cpf_cnpj,
					tipo_pessoa,
					tipo_devedor,
					nome_devedor,
					uf_unidade_responsavel,
					unidade_responsavel,
					entidade_responsavel,
					unidade_inscricao,
					numero_inscricao,
					tipo_situacao_inscricao,
					situacao_inscricao,
					receita_principal,
					null as tipo_credito,
					data_inscricao,
					indicador_ajuizado,
					valor_consolidado,
					'FGTS' as arquivo_origem
				from pgfn_fgts
				union all select
					cpf_cnpj,
					tipo_pessoa,
					tipo_devedor,
					nome_devedor,
					uf_unidade_responsavel,
					unidade_responsavel,
					null as entidade_responsavel,
					null as unidade_inscricao,
					numero_inscricao,
					tipo_situacao_inscricao,
					situacao_inscricao,
					receita_principal,
					null as tipo_credito,
					data_inscricao,
					indicador_ajuizado,
					valor_consolidado,
					'PREVIDENCIARIO' as arquivo_origem
				from pgfn_previdenciario
				union all select
					cpf_cnpj,
					tipo_pessoa,
					tipo_devedor,
					nome_devedor,
					uf_unidade_responsavel,
					unidade_responsavel,
					null as entidade_responsavel,
					null as unidade_inscricao,
					numero_inscricao,
					tipo_situacao_inscricao,
					situacao_inscricao,
					null as receita_principal,
					tipo_credito,
					data_inscricao,
					indicador_ajuizado,
					valor_consolidado,
					'GERAL' as arquivo_origem
				from pgfn_nao_previdenciario""");
		Logger.info("View criada em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void criaTabelaFGTS(Path diretorio, Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Crianto tabela de dados do FGTS...");
		Common.executeUpdateStatement(conn, """
				CREATE TABLE pgfn_fgts
				(`cpf_cnpj` TEXT,
				`tipo_pessoa` TEXT,
				`tipo_devedor` TEXT,
				`nome_devedor` TEXT,
				`uf_unidade_responsavel` TEXT,
				`unidade_responsavel` TEXT,
				`entidade_responsavel` TEXT,
				`unidade_inscricao` TEXT,
				`numero_inscricao` TEXT,
				`tipo_situacao_inscricao` TEXT,
				`situacao_inscricao` TEXT,
				`receita_principal` TEXT,
				`data_inscricao` TEXT,
				`indicador_ajuizado` TEXT,
				`valor_consolidado` REAL)""");

		Logger.info("Inserindo linhas...");
		try (PreparedStatement stmt = conn.prepareStatement("""
				insert into pgfn_fgts
				(cpf_cnpj,
				tipo_pessoa,
				tipo_devedor,
				nome_devedor,
				uf_unidade_responsavel,
				unidade_responsavel,
				entidade_responsavel,
				unidade_inscricao,
				numero_inscricao,
				tipo_situacao_inscricao,
				situacao_inscricao,
				receita_principal,
				data_inscricao,
				indicador_ajuizado,
				valor_consolidado)
				values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""");
				BatchAux aux = new BatchAux(stmt);) {
			leBase(diretorio, this::linhaFGTS).forEach(aux::appendLinhaFGTS);
			Logger.info("Tabela criada em {}", Common.formatDuration(System.nanoTime()-t0));
		} finally {
			conn.commit();
		}
	}

	private void indiceTabelaFGTS(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando índice...");
		Common.executeUpdateStatement(conn, "CREATE INDEX `index_fgts` ON `pgfn_fgts` (`cpf_cnpj`)");
		Logger.info("Índice criado {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void criaTabelaPrevidenciaria(Path diretorio, Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Crianto tabela de dados previdenciários...");
		Common.executeUpdateStatement(conn, """
				CREATE TABLE pgfn_previdenciario
				(`cpf_cnpj` TEXT,
				`tipo_pessoa` TEXT,
				`tipo_devedor` TEXT,
				`nome_devedor` TEXT,
				`uf_unidade_responsavel` TEXT,
				`unidade_responsavel` TEXT,
				`numero_inscricao` TEXT,
				`tipo_situacao_inscricao` TEXT,
				`situacao_inscricao` TEXT,
				`receita_principal` TEXT,
				`data_inscricao` TEXT,
				`indicador_ajuizado` TEXT,
				`valor_consolidado` REAL)""");

		Logger.info("Inserindo linhas...");
		try (PreparedStatement stmt = conn.prepareStatement("""
				insert into pgfn_previdenciario
				(cpf_cnpj,
				tipo_pessoa,
				tipo_devedor,
				nome_devedor,
				uf_unidade_responsavel,
				unidade_responsavel,
				numero_inscricao,
				tipo_situacao_inscricao,
				situacao_inscricao,
				receita_principal,
				data_inscricao,
				indicador_ajuizado,
				valor_consolidado)
				values (?,?,?,?,?,?,?,?,?,?,?,?,?)""");
				BatchAux aux = new BatchAux(stmt);) {
			leBase(diretorio, this::linhaPrevidenciaria)
					.forEach(aux::appendLinhaPrevidenciaria);
			Logger.info("Linhas inseridas {}", Common.formatDuration(System.nanoTime()-t0));
		} finally {
			conn.commit();
		}
	}

	private void indiceTabelaPrevidenciaria(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando índice...");
		Common.executeUpdateStatement(conn, "CREATE INDEX `index_previdenciario` ON `pgfn_previdenciario` (`cpf_cnpj`)");
		Logger.info("Índice criado {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void criaTabelaNaoPrevidenciaria(Path diretorio, Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Crianto tabela de dados não previdenciários...");
		Common.executeUpdateStatement(conn, """
				CREATE TABLE pgfn_nao_previdenciario
				(`cpf_cnpj` TEXT,
				`tipo_pessoa` TEXT,
				`tipo_devedor` TEXT,
				`nome_devedor` TEXT,
				`uf_unidade_responsavel` TEXT,
				`unidade_responsavel` TEXT,
				`numero_inscricao` TEXT,
				`tipo_situacao_inscricao` TEXT,
				`situacao_inscricao` TEXT,
				`tipo_credito` TEXT,
				`data_inscricao` TEXT,
				`indicador_ajuizado` TEXT,
				`valor_consolidado` REAL)""");

		Logger.info("Inserindo linhas...");
		try (PreparedStatement stmt = conn.prepareStatement("""
				insert into pgfn_nao_previdenciario
				(cpf_cnpj,
				tipo_pessoa,
				tipo_devedor,
				nome_devedor,
				uf_unidade_responsavel,
				unidade_responsavel,
				numero_inscricao,
				tipo_situacao_inscricao,
				situacao_inscricao,
				tipo_credito,
				data_inscricao,
				indicador_ajuizado,
				valor_consolidado)
				values (?,?,?,?,?,?,?,?,?,?,?,?,?)""");
				BatchAux aux = new BatchAux(stmt);) {
			leBase(diretorio, this::linhaNaoPrevidenciaria)
					.forEach(aux::appendLinhaNaoPrevidenciaria);
			Logger.info("Linhas inseridas {}", Common.formatDuration(System.nanoTime()-t0));
		} finally {
			conn.commit();
		}
	}

	private void indiceTabelaNaoPrevidenciaria(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando índice...");
		Common.executeUpdateStatement(conn, "CREATE INDEX `index_nao_previdenciario` ON `pgfn_nao_previdenciario` (`cpf_cnpj`)");
		Logger.info("Índice criado {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private <T> Stream<T> leBase(Path diretorio, Function<String,T> mapeamento) {
		try {
			return Files
					.list(diretorio)
					.flatMap(arquivo -> leArquivoIndividual(arquivo, mapeamento));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private <T> Stream<T> leArquivoIndividual(Path entrada, Function<String,T> mapeamento) {
		try {
			return Files
					.lines(entrada, Common.CHARSET)
					.skip(1)
					.map(mapeamento);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private LinhaFGTS linhaFGTS(String linha) {
		String[] componentes = linha.toUpperCase().split(";");
		if (componentes.length != 15) {
			throw new RuntimeException("Formato do arquivo FGTS diferente do esperado.");
		}

		return new LinhaFGTS(
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
				new BigDecimal(componentes[14])  //VALOR_CONSOLIDADO
				);
	}

	private LinhaPrevidenciaria linhaPrevidenciaria(String linha) {
		String[] componentes = linha.toUpperCase().split(";");
		if (componentes.length != 13) {
			throw new RuntimeException("Formato do arquivo previdenciário diferente do esperado.");
		}

		return new LinhaPrevidenciaria(
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
				new BigDecimal(componentes[12])  //VALOR_CONSOLIDADO
				);
	}

	private LinhaNaoPrevidenciaria linhaNaoPrevidenciaria(String linha) {
		String[] componentes = linha.toUpperCase().split(";");
		if (componentes.length != 13) {
			throw new RuntimeException("Formato do arquivo não previdenciário diferente do esperado.");
		}

		return new LinhaNaoPrevidenciaria(
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
				new BigDecimal(componentes[12])  //VALOR_CONSOLIDADO
				);
	}

	private class BatchAux implements Closeable {
		private final PreparedStatement stmt;
		private int index;

		public BatchAux(PreparedStatement stmt) {
			this.stmt = stmt;
		}

		public void appendLinhaFGTS(LinhaFGTS linha) {
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
				this.stmt.setString(13, linha.dataInscricao().strip());
				this.stmt.setString(14, linha.indicadorAjuizado().strip());
				this.stmt.setBigDecimal(15, linha.valor());
				this.stmt.addBatch();

				this.index += 1;
				if (this.index % BasePGFN.this.batchSize == 0) {
					this.stmt.executeBatch();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public void appendLinhaPrevidenciaria(LinhaPrevidenciaria linha) {
			try {
				this.stmt.setString(1, linha.cpfCnpj().replace(".", "").replace("/", "").replace("-", "").strip());
				this.stmt.setString(2, linha.tipoPessoa().strip());
				this.stmt.setString(3, linha.tipoDevedor().strip());
				this.stmt.setString(4, linha.nomeDevedor().strip());
				this.stmt.setString(5, linha.ufUnidadeResponsavel().strip());
				this.stmt.setString(6, linha.unidadeResponsavel().strip());
				this.stmt.setString(7, linha.numeroInscricao().strip());
				this.stmt.setString(8, linha.tipoSituacaoInscricao().strip());
				this.stmt.setString(9, linha.situacaoInscricao().strip());
				this.stmt.setString(10, linha.receitaPrincipal().strip());
				this.stmt.setString(11, linha.dataInscricao().strip());
				this.stmt.setString(12, linha.indicadorAjuizado().strip());
				this.stmt.setBigDecimal(13, linha.valor());
				this.stmt.addBatch();

				this.index += 1;
				if (this.index % BasePGFN.this.batchSize == 0) {
					this.stmt.executeBatch();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		public void appendLinhaNaoPrevidenciaria(LinhaNaoPrevidenciaria linha) {
			try {
				this.stmt.setString(1, linha.cpfCnpj().replace(".", "").replace("/", "").replace("-", "").strip());
				this.stmt.setString(2, linha.tipoPessoa().strip());
				this.stmt.setString(3, linha.tipoDevedor().strip());
				this.stmt.setString(4, linha.nomeDevedor().strip());
				this.stmt.setString(5, linha.ufUnidadeResponsavel().strip());
				this.stmt.setString(6, linha.unidadeResponsavel().strip());
				this.stmt.setString(7, linha.numeroInscricao().strip());
				this.stmt.setString(8, linha.tipoSituacaoInscricao().strip());
				this.stmt.setString(9, linha.situacaoInscricao().strip());
				this.stmt.setString(10, linha.tipoCredito().strip());
				this.stmt.setString(11, linha.dataInscricao().strip());
				this.stmt.setString(12, linha.indicadorAjuizado().strip());
				this.stmt.setBigDecimal(13, linha.valor());
				this.stmt.addBatch();

				this.index += 1;
				if (this.index % BasePGFN.this.batchSize == 0) {
					this.stmt.executeBatch();
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() {
			try {
				this.stmt.executeBatch();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
