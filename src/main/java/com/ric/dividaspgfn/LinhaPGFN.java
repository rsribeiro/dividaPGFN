package com.ric.dividaspgfn;

import java.math.BigDecimal;

final class LinhaPGFN {
	private final String cpfCnpj;
	private final String tipoPessoa;
	private final String tipoDevedor;
	private final String nomeDevedor;
	private final String ufUnidadeResponsavel;
	private final String unidadeResponsavel;
	private final String entidadeResponsavel;
	private final String unidadeInscricao;
	private final String numeroInscricao;
	private final String tipoSituacaoInscricao;
	private final String situacaoInscricao;
	private final String receitaPrincipal;
	private final String tipoCredito;
	private final String dataInscricao;
	private final String indicadorAjuizado;
	private final BigDecimal valor;
	private final String arquivoOrigem;

	public LinhaPGFN(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel, String entidadeResponsavel, String unidadeInscricao,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
			String tipoCredito, String dataInscricao, String indicadorAjuizado, String valor,
			String arquivoOrigem) {
		this.cpfCnpj = cpfCnpj;
		this.tipoPessoa = tipoPessoa;
		this.tipoDevedor = tipoDevedor;
		this.nomeDevedor = nomeDevedor;
		this.ufUnidadeResponsavel = ufUnidadeResponsavel;
		this.unidadeResponsavel = unidadeResponsavel;
		this.entidadeResponsavel = entidadeResponsavel;
		this.unidadeInscricao = unidadeInscricao;
		this.numeroInscricao = numeroInscricao;
		this.tipoSituacaoInscricao = tipoSituacaoInscricao;
		this.situacaoInscricao = situacaoInscricao;
		this.receitaPrincipal = receitaPrincipal;
		this.tipoCredito = tipoCredito;
		this.dataInscricao = dataInscricao;
		this.indicadorAjuizado = indicadorAjuizado;
		this.valor = new BigDecimal(valor);
		this.arquivoOrigem = arquivoOrigem;
	}

	public static LinhaPGFN linhaFGTS(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel, String entidadeResponsavel, String unidadeInscricao,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
			String dataInscricao, String indicadorAjuizado, String valor) {
		return new LinhaPGFN(cpfCnpj, tipoPessoa, tipoDevedor, nomeDevedor, ufUnidadeResponsavel, unidadeResponsavel, entidadeResponsavel, unidadeInscricao, numeroInscricao, tipoSituacaoInscricao, situacaoInscricao, receitaPrincipal, "", dataInscricao, indicadorAjuizado, valor, "FGTS");
	}

	public static LinhaPGFN linhaPrevidenciaria(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
			String dataInscricao, String indicadorAjuizado, String valor) {
		return new LinhaPGFN(cpfCnpj, tipoPessoa, tipoDevedor, nomeDevedor, ufUnidadeResponsavel, unidadeResponsavel, "", "", numeroInscricao, tipoSituacaoInscricao, situacaoInscricao, receitaPrincipal, "", dataInscricao, indicadorAjuizado, valor, "PREVIDENCIARIO");
	}

	public static LinhaPGFN linhaGeral(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao,
			String tipoCredito, String dataInscricao, String indicadorAjuizado, String valor) {
		return new LinhaPGFN(cpfCnpj, tipoPessoa, tipoDevedor, nomeDevedor, ufUnidadeResponsavel, unidadeResponsavel, "", "", numeroInscricao, tipoSituacaoInscricao, situacaoInscricao, "", tipoCredito, dataInscricao, indicadorAjuizado, valor, "GERAL");
	}

	public String getCpfCnpj() {
		return this.cpfCnpj;
	}

	public String getTipoPessoa() {
		return this.tipoPessoa;
	}

	public String getTipoDevedor() {
		return this.tipoDevedor;
	}

	public String getNomeDevedor() {
		return this.nomeDevedor;
	}

	public String getUfUnidadeResponsavel() {
		return this.ufUnidadeResponsavel;
	}

	public String getUnidadeResponsavel() {
		return this.unidadeResponsavel;
	}

	public String getEntidadeResponsavel() {
		return this.entidadeResponsavel;
	}

	public String getUnidadeInscricao() {
		return this.unidadeInscricao;
	}

	public String getNumeroInscricao() {
		return this.numeroInscricao;
	}

	public String getTipoSituacaoInscricao() {
		return this.tipoSituacaoInscricao;
	}

	public String getSituacaoInscricao() {
		return this.situacaoInscricao;
	}

	public String getReceitaPrincipal() {
		return this.receitaPrincipal;
	}

	public String getTipoCredito() {
		return this.tipoCredito;
	}

	public String getDataInscricao() {
		return this.dataInscricao;
	}

	public String getIndicadorAjuizado() {
		return this.indicadorAjuizado;
	}

	public BigDecimal getValor() {
		return this.valor;
	}

	public String getArquivoOrigem() {
		return this.arquivoOrigem;
	}
}
