package com.ric.dividapgfn;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import org.tinylog.Logger;

import com.ric.dividapgfn.csv.CSVWriter;
import com.ric.dividapgfn.util.Common;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "filtra_divida", aliases = { "filtra", "filtro" }, description = "Filtra a base da PGFN de acordo com os critérios definidos")
public final class FiltraDivida implements Callable<Integer> {
	@Option(names = { "-d", "--dir" }, required = true, description = "Diretório base")
	private Path dirBase;

	@Option(names = { "-b", "--basecnpj" }, required = true, description = "Arquivo sqlite com a base do CNPJ")
	private Path baseCNPJ;

	@Option(names = { "-c", "--consulta" }, required = true, description = "Consulta sql para determinar os CNPJs a serem buscados no arquivo da PGFN")
	private Path arqConsulta;

	@Option(names = { "-s", "--separador" }, description = "Separador a ser utilizado nos arquivos CSV exportados (padrão=${DEFAULT-VALUE})" )
	private char separador = ';';

	@Override
	public Integer call() {
		long t0 = System.nanoTime();

		Path basePGFN = this.dirBase.resolve("entrada/pgfn.sqlite");
		Path dirSaida = this.dirBase.resolve("saida");
		try {
			Files.createDirectories(dirSaida);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

		Logger.info("Criando arquivos de análise em {}", dirSaida.toAbsolutePath());

		if (Files.notExists(basePGFN)) {
			Logger.error("Erro: arquivo {} não encontrado.", basePGFN.toAbsolutePath());
			return CommandLine.ExitCode.USAGE;
		}

		if (Files.notExists(this.baseCNPJ)) {
			Logger.error("Erro: arquivo {} não encontrado.", this.baseCNPJ.toAbsolutePath());
			return CommandLine.ExitCode.USAGE;
		}

		if (Files.notExists(this.arqConsulta)) {
			Logger.error("Erro: arquivo {} não encontrado.", this.arqConsulta.toAbsolutePath());
			return CommandLine.ExitCode.USAGE;
		}

		try {
			Logger.info("Conectando base CNPJ...");
			Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.baseCNPJ.toAbsolutePath());
			Logger.info("Conectando base PGFN...");
			Common.executeUpdateStatement(conn, "ATTACH '" + basePGFN.toAbsolutePath() + "' AS `pgfn` KEY ''");

			try {
				String consulta = Files.readString(this.arqConsulta, Common.CHARSET);

				tabelaCNPJ(conn, consulta);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}

			indiceTabelaCNPJ(conn);
			tabelaInscricoesDivida(conn);
			indiceTabelaInscricoesDivida(conn);
			tabelaDivida(conn);
			indiceTabelaDivida(conn);
			exportaBaseDivida(conn, dirSaida.resolve("divida.csv"));
			exportaCadastroCNPJ(conn, dirSaida.resolve("cnpj.csv"));
			exportaCadastroSocios(conn, dirSaida.resolve("socios.csv"));
			exportaTabelaCorresponsaveis(conn, dirSaida.resolve("corresponsaveis.csv"));
		} catch (SQLException e) {
			Logger.error("Erro SQL: {}", e.getLocalizedMessage());
			return CommandLine.ExitCode.SOFTWARE;
		}

		Logger.info("Programa executado em {}", Common.formatDuration(System.nanoTime()-t0));

		return CommandLine.ExitCode.OK;
	}

	private void tabelaCNPJ(Connection conn, String consulta) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando tabela temporária com CNPJs...");
		Common.executeUpdateStatement(conn, "create table temp.cnpj as " + consulta);
		Logger.info("Tabela criada em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void indiceTabelaCNPJ(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando índice...");
		Common.executeUpdateStatement(conn, "CREATE INDEX `temp.index_tmp_cnpj` ON `cnpj` (`cnpj_basico`)");
		Logger.info("Índice criado em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void tabelaInscricoesDivida(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando tabela temporária com inscrições da dívida relevantes");
		Common.executeUpdateStatement(conn, """
				create table temp.inscricao_divida as
				select distinct dev.numero_inscricao
				from temp.cnpj cnpj
				join pgfn.pgfn_devedores dev
				on cnpj.cnpj_basico = substr(dev.cpf_cnpj,1,8)""");
		Logger.info("Tabela criada em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void indiceTabelaInscricoesDivida(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando índice...");
		Common.executeUpdateStatement(conn, "CREATE INDEX `temp.index_tmp_insc_divida` ON `inscricao_divida` (`numero_inscricao`)");
		Logger.info("Índice criado em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void tabelaDivida(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando tabela temporária das dívidas");
		Common.executeUpdateStatement(conn, """
				create table temp.divida as
				select
					dev.*,
					case when dev.tipo_pessoa = 'PESSOA JURÍDICA' then substr(dev.cpf_cnpj,1,8) else dev.cpf_cnpj end as cnpj_matriz
				from temp.inscricao_divida div
				join pgfn.pgfn_devedores dev
				on div.numero_inscricao = dev.numero_inscricao""");
		Logger.info("Tabela criada em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void indiceTabelaDivida(Connection conn) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Criando índices...");
		Common.executeUpdateStatement(conn, "CREATE INDEX `temp.index_tmp_divida_cnpj` ON `divida` (`cnpj_matriz`)");
		Common.executeUpdateStatement(conn, "CREATE INDEX `temp.index_tmp_divida_insc` ON `divida` (`numero_inscricao`)");
		Logger.info("Índices criados em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void exportaBaseDivida(Connection conn, Path arquivo) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Exportando base da dívida...");
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select * from temp.divida order by CPF_CNPJ, DATA_INSCRICAO, NUMERO_INSCRICAO");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			CSVWriter writer = new CSVWriter(this.separador);
			writer.write(rs, out, true);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		Logger.info("Base exportada em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void exportaCadastroCNPJ(Connection conn, Path arquivo) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Exportando base de cadastro CNPJ...");
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("""
						select
							est.cnpj,
							emp.cnpj_basico,
							est.matriz_filial,
							emp.razao_social,
							est.nome_fantasia,
							est.situacao_cadastral,
							est.data_situacao_cadastral,
							est.motivo_situacao_cadastral,
							est.nome_cidade_exterior,
							est.pais as cd_pais,
							pais.descricao as nm_pais,
							emp.natureza_juridica as cd_natureza_juridica,
							nj.descricao as nm_natureza_juridica,
							est.data_inicio_atividades,
							est.cnae_fiscal,
							cnae.descricao as nm_cnae,
							est.cnae_fiscal_secundaria,
							est.tipo_logradouro,
							est.logradouro,
							est.numero,
							est.complemento,
							est.bairro,
							est.cep,
							est.uf,
							est.municipio,
							est.ddd1,
							est.telefone1,
							est.ddd2,
							est.telefone2,
							est.ddd_fax,
							est.fax,
							est.correio_eletronico,
							est.situacao_especial,
							est.data_situacao_especial,
							emp.qualificacao_responsavel,
							emp.capital_social,
							emp.porte_empresa,
							emp.ente_federativo_responsavel
						from empresas emp
						join estabelecimento est on emp.cnpj_basico = est.cnpj_basico
						left outer join cnae cnae on est.cnae_fiscal = cnae.codigo
						left outer join natureza_juridica nj on emp.natureza_juridica = nj.codigo
						left outer join pais pais on est.pais = pais.codigo
						where emp.cnpj_basico in (select distinct cnpj_matriz from temp.divida where tipo_pessoa = 'PESSOA JURÍDICA')
						order by est.cnpj""");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			CSVWriter writer = new CSVWriter(this.separador);
			writer.write(rs, out, true);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		Logger.info("Base exportada em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void exportaCadastroSocios(Connection conn, Path arquivo) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Exportando cadastro de sócios...");
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("""
						select
							est.cnpj,
							est.cnpj_basico,
							est.matriz_filial,
							emp.razao_social,
							est.nome_fantasia,
							soc.identificador_de_socio,
							soc.nome_socio,
							soc.cnpj_cpf_socio,
							soc.qualificacao_socio,
							qs.descricao as nm_qualificacao_socio,
							soc.data_entrada_sociedade,
							soc.pais,
							soc.representante_legal,
							soc.nome_representante,
							soc.qualificacao_representante_legal,
							soc.faixa_etaria
						from estabelecimento est
							join empresas emp on est.cnpj_basico = emp.cnpj_basico
							left outer join socios soc on est.cnpj = soc.cnpj
							left outer join qualificacao_socio qs on soc.qualificacao_socio = qs.codigo
						where est.cnpj_basico in (select distinct cnpj_matriz from temp.divida)
						order by soc.cnpj, soc.identificador_de_socio""");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			CSVWriter writer = new CSVWriter(this.separador);
			writer.write(rs, out, true);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		Logger.info("Cadastro exportado em {}", Common.formatDuration(System.nanoTime()-t0));
	}

	private void exportaTabelaCorresponsaveis(Connection conn, Path arquivo) throws SQLException {
		long t0 = System.nanoTime();
		Logger.info("Exportando tabela de corresponsaveis...");
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("""
						with t_total as (select cnpj_matriz, sum(valor_consolidado) as valor from temp.divida where tipo_devedor='PRINCIPAL' group by cnpj_matriz)
						select
							p.cnpj_matriz as cpf_cnpj_principal, p.nome_devedor as nome_devedor_principal,
							s.cnpj_matriz as cpf_cnpj_secundario, s.nome_devedor as nome_devedor_secundario,
							sum(p.valor_consolidado) as valor_corresponsabilidade,
							t.valor as divida_devedor_principal
						from temp.divida p
							join temp.divida s on p.numero_inscricao = s.numero_inscricao
							join t_total t on p.cnpj_matriz = t.cnpj_matriz
						where p.tipo_devedor = 'PRINCIPAL' and s.tipo_devedor <> 'PRINCIPAL'
						group by p.cnpj_matriz, p.nome_devedor, s.cnpj_matriz, s.nome_devedor
						order by divida_devedor_principal desc, valor_corresponsabilidade desc, cpf_cnpj_secundario""");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			CSVWriter writer = new CSVWriter(this.separador);
			writer.write(rs, out, true);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		Logger.info("Tabela exportada em {}", Common.formatDuration(System.nanoTime()-t0));
	}
}
