package com.ric.dividaspgfn;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "filtra_divida", aliases = { "filtra", "filtro" }, description = "Filtra a base da PGFN de acordo com os critérios definidos")
final class FiltraDivida implements Callable<Integer> {
	private static final DecimalFormat NUMBER_FORMAT;

	static {
		DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
		symbols.setGroupingSeparator('.');
		symbols.setDecimalSeparator(',');
		NUMBER_FORMAT = new DecimalFormat("#.###", symbols);
	}

	@Option(names = { "-d", "--dir" }, required = true, description = "Diretório base")
	private Path dirBase;

	@Option(names = { "-b", "--basecnpj" }, required = true, description = "Arquivo sqlite com a base do CNPJ")
	private Path baseCNPJ;

	@Option(names = { "-c", "--consulta" }, required = true, description = "Consulta sql para determinar os CNPJs a serem buscados no arquivo da PGFN")
	private Path arqConsulta;

	@Option(names = { "-s", "--separador" }, description = "Separador a ser utilizado nos arquivos CSV exportados (padrão=${DEFAULT-VALUE})" )
	private String separador = ";";

	@Override
	public Integer call() throws Exception {
		Path basePGFN = this.dirBase.resolve("entrada/pgfn.sqlite");
		Path dirSaida = this.dirBase.resolve("saida");
		if (Files.notExists(dirSaida)) {
			Files.createDirectory(dirSaida);
		}

		Common.mensagemProgresso("Criando arquivos de análise em " + dirSaida.toAbsolutePath());

		if (Files.notExists(basePGFN)) {
			Common.mensagemErro("Erro: arquivo " + basePGFN.toAbsolutePath() + " não encontrado.");
			return 1;
		}

		if (Files.notExists(this.baseCNPJ)) {
			Common.mensagemErro("Erro: arquivo " + this.baseCNPJ.toAbsolutePath() + " não encontrado.");
			return 1;
		}

		if (Files.notExists(this.arqConsulta)) {
			Common.mensagemErro("Erro: arquivo " + this.arqConsulta.toAbsolutePath() + " não encontrado.");
			return 1;
		}

		System.out.println("Conectando base CNPJ...");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.baseCNPJ.toAbsolutePath());
		System.out.println("Conectando base PGFN...");
		Common.executeUpdateStatement(conn, "ATTACH '" + basePGFN.toAbsolutePath() + "' AS `pgfn` KEY ''");

		String consulta = Files.readString(this.arqConsulta, Common.CHARSET);

		System.out.println("Criando tabela temporária com CNPJs...");
		tabelaCNPJ(conn, consulta);
		System.out.println("Tabela criada.");

		System.out.println("Criando tabela temporária com inscrições da dívida relevantes");
		tabelaInscricoesDivida(conn);
		System.out.println("Tabela criada.");

		System.out.println("Criando tabela temporária das dívidas");
		tabelaDivida(conn);
		System.out.println("Tabela criada.");

		System.out.println("Exportando base da dívida...");
		exportaBaseDivida(conn, dirSaida.resolve("divida.csv"));
		System.out.println("Base exportada.");

		System.out.println("Exportando base de cadastro CNPJ...");
		exportaCadastroCNPJ(conn, dirSaida.resolve("cnpj.csv"));
		System.out.println("Base exportada.");

		System.out.println("Exportando cadastro de sócios...");
		exportaCadastroSocios(conn, dirSaida.resolve("socios.csv"));
		System.out.println("Cadastro exportado.");

		System.out.println("Exportando cadastro de CNAEs secundárias...");
		exportaCadastroCNAEsSecundarias(conn, dirSaida.resolve("cnae_secundaria.csv"));
		System.out.println("Cadastro exportado.");

		System.out.println("Exportando tabela de corresponsaveis...");
		exportaTabelaCorresponsaveis(conn, dirSaida.resolve("corresponsaveis.csv"));
		System.out.println("Tabela exportada.");

		return 0;
	}

	private void tabelaCNPJ(Connection conn, String consulta) throws SQLException {
		Common.executeUpdateStatement(conn, "create table temp.cnpj as " + consulta);
	}

	private void tabelaInscricoesDivida(Connection conn) throws SQLException {
		Common.executeUpdateStatement(conn, "create table temp.inscricao_divida as " +
				"select distinct numero_inscricao " +
				"from pgfn.pgfn_devedores " +
				"where substr(cpf_cnpj,1,8) in (select cnpj_matriz from temp.cnpj)");
	}

	private void tabelaDivida(Connection conn) throws SQLException {
		Common.executeUpdateStatement(conn, "create table temp.divida as " +
				"select *, " +
				"case when tipo_pessoa='PESSOA JURÍDICA' then substr(cpf_cnpj,1,8) else cpf_cnpj end as cnpj_matriz " +
				"from pgfn.pgfn_devedores " +
				"where numero_inscricao in (select numero_inscricao from temp.inscricao_divida)");
	}

	private void exportaBaseDivida(Connection conn, Path arquivo) throws SQLException, IOException {
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select * from temp.divida order by CPF_CNPJ, DATA_INSCRICAO, NUMERO_INSCRICAO");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			out.write("cpf_cnpj;tipo_pessoa;tipo_devedor;nome_devedor;uf_unidade_responsavel;unidade_responsavel;entidade_responsavel;unidade_inscricao;numero_inscricao;tipo_situacao_inscricao;situacao_inscricao;receita_principal;tipo_credito;data_inscricao;indicador_ajuizado;valor_consolidado;arquivo_origem;cpf_cnpj_matriz");
			out.newLine();

			while (rs.next()) {
				out.append(new StringJoiner(this.separador)
						.add('"'+rs.getString(1)+'"')
						.add('"'+rs.getString(2)+'"')
						.add('"'+rs.getString(3)+'"')
						.add('"'+rs.getString(4)+'"')
						.add('"'+rs.getString(5)+'"')
						.add('"'+rs.getString(6)+'"')
						.add('"'+rs.getString(7)+'"')
						.add('"'+rs.getString(8)+'"')
						.add('"'+rs.getString(9)+'"')
						.add('"'+rs.getString(10)+'"')
						.add('"'+rs.getString(11)+'"')
						.add('"'+rs.getString(12)+'"')
						.add('"'+rs.getString(13)+'"')
						.add('"'+rs.getString(14)+'"')
						.add('"'+rs.getString(15)+'"')
						.add(NUMBER_FORMAT.format(rs.getBigDecimal(16)))
						.add('"'+rs.getString(17)+'"')
						.add('"'+rs.getString(18)+'"')
						.toString());
				out.newLine();
			}
		}
	}

	private void exportaCadastroCNPJ(Connection conn, Path arquivo) throws SQLException, IOException {
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select " +
						"c.cnpj, " +
						"substr(cnpj,1,8) as cnpj_matriz, " +
						"c.identificador_matriz_filial, " +
						"c.razao_social, " +
						"c.nome_fantasia, " +
						"c.situacao_cadastral, " +
						"c.data_situacao_cadastral, " +
						"c.motivo_situacao_cadastral, " +
						"c.nm_cidade_exterior, " +
						"c.cod_pais, " +
						"c.nm_pais, " +
						"nj.cod_natureza_juridica, " +
						"nj.nm_natureza_juridica, " +
						"c.codigo_natureza_juridica, " +
						"nj.nm_subclass_natureza_juridica, " +
						"c.data_inicio_atividade, " +
						"c.cnae_fiscal, " +
						"replace(cnae.nm_cnae,char(10),' ') as nm_cnae, " +
						"c.descricao_tipo_logradouro, " +
						"c.logradouro, " +
						"c.numero, " +
						"c.complemento, " +
						"c.bairro, " +
						"c.cep, " +
						"c.uf, " +
						"c.codigo_municipio, " +
						"c.municipio, " +
						"c.ddd_telefone_1, " +
						"c.ddd_telefone_2, " +
						"c.ddd_fax, " +
						"c.correio_eletronico, " +
						"c.qualificacao_responsavel, " +
						"c.capital_social_empresa, " +
						"c.porte_empresa, " +
						"c.opcao_pelo_simples, " +
						"c.data_opcao_pelo_simples, " +
						"c.data_exclusao_simples, " +
						"c.opcao_pelo_mei, " +
						"c.situacao_especial, " +
						"c.data_situacao_especial " +
						"from cnpj_dados_cadastrais_pj c " +
						"left outer join tab_cnae cnae on c.cnae_fiscal = cnae.cod_cnae " +
						"left outer join tab_natureza_juridica nj on c.codigo_natureza_juridica = nj.cod_subclass_natureza_juridica " +
						"where substr(cnpj,1,8) in ( " +
						"select cnpj_matriz " +
						"from temp.divida " +
						"where tipo_pessoa='PESSOA JURÍDICA'" +
						") order by c.cnpj");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			out.write("cnpj;cnpj_matriz;identificador_matriz_filial;razao_social;nome_fantasia;situacao_cadastral;data_situacao_cadastral;motivo_situacao_cadastral;nm_cidade_exterior;cod_pais;nm_pais;cod_natureza_juridica;nm_natureza_juridica;cod_subclass_natureza_juridica;nm_subclass_natureza_juridica;data_inicio_atividade;cod_cnae;nm_cnae;descricao_tipo_logradouro;logradouro;numero;complemento;bairro;cep;uf;codigo_municipio;municipio;ddd_telefone_1;ddd_telefone_2;ddd_fax;correio_eletronico;qualificacao_responsavel;capital_social_empresa;porte_empresa;opcao_pelo_simples;data_opcao_pelo_simples;data_exclusao_simples;opcao_pelo_mei;situacao_especial;data_situacao_especial");
			out.newLine();

			while (rs.next()) {
				out.append(new StringJoiner(this.separador)
						.add('"'+rs.getString(1)+'"')
						.add('"'+rs.getString(2)+'"')
						.add('"'+rs.getString(3)+'"')
						.add('"'+rs.getString(4)+'"')
						.add('"'+rs.getString(5)+'"')
						.add('"'+rs.getString(6)+'"')
						.add('"'+rs.getString(7)+'"')
						.add('"'+rs.getString(8)+'"')
						.add('"'+rs.getString(9)+'"')
						.add('"'+rs.getString(10)+'"')
						.add('"'+rs.getString(11)+'"')
						.add('"'+rs.getString(12)+'"')
						.add('"'+rs.getString(13)+'"')
						.add('"'+rs.getString(14)+'"')
						.add('"'+rs.getString(15)+'"')
						.add('"'+rs.getString(16)+'"')
						.add('"'+rs.getString(17)+'"')
						.add('"'+rs.getString(18)+'"')
						.add('"'+rs.getString(19)+'"')
						.add('"'+rs.getString(20)+'"')
						.add('"'+rs.getString(21)+'"')
						.add('"'+rs.getString(22)+'"')
						.add('"'+rs.getString(23)+'"')
						.add('"'+rs.getString(24)+'"')
						.add('"'+rs.getString(25)+'"')
						.add('"'+rs.getString(26)+'"')
						.add('"'+rs.getString(27)+'"')
						.add('"'+rs.getString(28)+'"')
						.add('"'+rs.getString(29)+'"')
						.add('"'+rs.getString(30)+'"')
						.add('"'+rs.getString(31)+'"')
						.add('"'+rs.getString(32)+'"')
						.add('"'+rs.getString(33)+'"')
						.add(NUMBER_FORMAT.format(rs.getBigDecimal(34)))
						.add('"'+rs.getString(35)+'"')
						.add('"'+rs.getString(36)+'"')
						.add('"'+rs.getString(37)+'"')
						.add('"'+rs.getString(38)+'"')
						.add('"'+rs.getString(39)+'"')
						.add('"'+rs.getString(40)+'"')
						.toString());
				out.newLine();
			}
		}
	}

	private void exportaCadastroSocios(Connection conn, Path arquivo) throws SQLException, IOException {
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select " +
						"s.cnpj, " +
						"p.identificador_matriz_filial, " +
						"p.razao_social, " +
						"p.nome_fantasia, " +
						"s.identificador_socio, " +
						"s.nome_socio, " +
						"s.cnpj_cpf_socio, " +
						"s.cod_qualificacao_socio, " +
						"qs.nm_qualificacao_responsavel_socio, " +
						"s.data_entrada_sociedade, " +
						"s.cpf_representante_legal, " +
						"s.nome_representante, " +
						"s.cod_qualificacao_representante_legal " +
						"from cnpj_dados_socios_pj s " +
						"left outer join cnpj_dados_cadastrais_pj p on s.cnpj = p.cnpj " +
						"left outer join tab_qualificacao_responsavel_socio qs on s.cod_qualificacao_socio = qs.cod_qualificacao_responsavel_socio " +
						"where substr(s.cnpj,1,8) in (select cnpj_matriz from temp.divida) " +
						"order by s.cnpj, s.identificador_socio");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			out.write("cnpj;identificador_matriz_filial;razao_social;nome_fantasia;identificador_socio;nome_socio;cnpj_cpf_socio;cod_qualificacao_socio;nm_qualificacao_responsavel_socio;data_entrada_sociedade;cpf_representante_legal;nome_representante;cod_qualificacao_representante_legal");
			out.newLine();

			while (rs.next()) {
				out.append(new StringJoiner(this.separador)
						.add('"'+rs.getString(1)+'"')
						.add('"'+rs.getString(2)+'"')
						.add('"'+rs.getString(3)+'"')
						.add('"'+rs.getString(4)+'"')
						.add('"'+rs.getString(5)+'"')
						.add('"'+rs.getString(6)+'"')
						.add('"'+rs.getString(7)+'"')
						.add('"'+rs.getString(8)+'"')
						.add('"'+rs.getString(9)+'"')
						.add('"'+rs.getString(10)+'"')
						.add('"'+rs.getString(11)+'"')
						.add('"'+rs.getString(12)+'"')
						.add('"'+rs.getString(13)+'"')
						.toString());
				out.newLine();
			}
		}
	}

	private void exportaCadastroCNAEsSecundarias(Connection conn, Path arquivo) throws SQLException, IOException {
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select " +
						"s.cnpj, " +
						"p.identificador_matriz_filial, " +
						"p.razao_social, " +
						"p.nome_fantasia, " +
						"s.cnae_secundario, " +
						"replace(cnae.nm_cnae,char(10),' ') " +
						"from cnpj_dados_cnae_secundario_pj s " +
						"left outer join cnpj_dados_cadastrais_pj p on s.cnpj = p.cnpj " +
						"left outer join tab_cnae cnae on s.cnae_secundario = cnae.cod_cnae " +
						"where substr(s.cnpj,1,8) in (select cnpj_matriz from temp.divida) " +
						"order by s.cnpj, s.cnae_secundario");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			out.write("cnpj;identificador_matriz_filial;razao_social;nome_fantasia;cnae_secundario;nm_cnae");
			out.newLine();

			while (rs.next()) {
				out.append(new StringJoiner(this.separador)
						.add('"'+rs.getString(1)+'"')
						.add('"'+rs.getString(2)+'"')
						.add('"'+rs.getString(3)+'"')
						.add('"'+rs.getString(4)+'"')
						.add('"'+rs.getString(5)+'"')
						.add('"'+rs.getString(6)+'"')
						.toString());
				out.newLine();
			}
		}
	}

	private void exportaTabelaCorresponsaveis(Connection conn, Path arquivo) throws SQLException, IOException {
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("with t_total as (select cnpj_matriz, sum(valor_consolidado) as valor from temp.divida where tipo_devedor='PRINCIPAL' group by cnpj_matriz)"
						+ "select "
						+ "p.cnpj_matriz as cpf_cnpj_principal, p.nome_devedor as nome_devedor_principal, "
						+ "s.cnpj_matriz as cpf_cnpj_secundario, s.nome_devedor as nome_devedor_secundario, "
						+ "sum(p.valor_consolidado) as valor_corresponsabilidade, "
						+ "t.valor as divida_devedor_principal "
						+ "from temp.divida p "
						+ "join temp.divida s on p.numero_inscricao = s.numero_inscricao "
						+ "join t_total t on p.cnpj_matriz = t.cnpj_matriz "
						+ "where p.tipo_devedor = 'PRINCIPAL' and s.tipo_devedor <> 'PRINCIPAL' "
						+ "group by p.cnpj_matriz, p.nome_devedor, s.cnpj_matriz, s.nome_devedor "
						+ "order by divida_devedor_principal desc, valor_corresponsabilidade desc, cpf_cnpj_secundario");
				BufferedWriter out = Files.newBufferedWriter(arquivo, Common.CHARSET);) {
			out.write("cpf_cnpj_principal;nm_devedor_principal;cpf_cnpj_secundario;nm_devedor_secundario;valor_corresponsabilidade;divida_devedor_principal");
			out.newLine();

			while (rs.next()) {
				out.append(new StringJoiner(this.separador)
						.add('"'+rs.getString(1)+'"')
						.add('"'+rs.getString(2)+'"')
						.add('"'+rs.getString(3)+'"')
						.add('"'+rs.getString(4)+'"')
						.add(NUMBER_FORMAT.format(rs.getBigDecimal(5)))
						.add(NUMBER_FORMAT.format(rs.getBigDecimal(6)))
						.toString());
				out.newLine();
			}
		}
	}
}
