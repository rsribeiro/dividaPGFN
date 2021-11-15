package com.ric.dividaspgfn;

import java.math.BigDecimal;

public record LinhaPGFN(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
		String ufUnidadeResponsavel, String unidadeResponsavel, String entidadeResponsavel, String unidadeInscricao,
		String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
		String tipoCredito, String dataInscricao, String indicadorAjuizado, BigDecimal valor,
		String arquivoOrigem) {
	public static LinhaPGFN linhaFGTS(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel, String entidadeResponsavel, String unidadeInscricao,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
			String dataInscricao, String indicadorAjuizado, String valor) {
		return new LinhaPGFN(cpfCnpj, tipoPessoa, tipoDevedor, nomeDevedor, ufUnidadeResponsavel, unidadeResponsavel, entidadeResponsavel, unidadeInscricao, numeroInscricao, tipoSituacaoInscricao, situacaoInscricao, receitaPrincipal, "", dataInscricao, indicadorAjuizado, new BigDecimal(valor), "FGTS");
	}

	public static LinhaPGFN linhaPrevidenciaria(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao, String receitaPrincipal,
			String dataInscricao, String indicadorAjuizado, String valor) {
		return new LinhaPGFN(cpfCnpj, tipoPessoa, tipoDevedor, nomeDevedor, ufUnidadeResponsavel, unidadeResponsavel, "", "", numeroInscricao, tipoSituacaoInscricao, situacaoInscricao, receitaPrincipal, "", dataInscricao, indicadorAjuizado, new BigDecimal(valor), "PREVIDENCIARIO");
	}

	public static LinhaPGFN linhaGeral(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
			String ufUnidadeResponsavel, String unidadeResponsavel,
			String numeroInscricao, String tipoSituacaoInscricao, String situacaoInscricao,
			String tipoCredito, String dataInscricao, String indicadorAjuizado, String valor) {
		return new LinhaPGFN(cpfCnpj, tipoPessoa, tipoDevedor, nomeDevedor, ufUnidadeResponsavel, unidadeResponsavel, "", "", numeroInscricao, tipoSituacaoInscricao, situacaoInscricao, "", tipoCredito, dataInscricao, indicadorAjuizado, new BigDecimal(valor), "GERAL");
	}
}
