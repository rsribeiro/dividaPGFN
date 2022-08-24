package com.ric.dividapgfn.linha;

import java.math.BigDecimal;

public record LinhaNaoPrevidenciaria(String cpfCnpj, String tipoPessoa, String tipoDevedor, String nomeDevedor,
		String ufUnidadeResponsavel, String unidadeResponsavel, String numeroInscricao, String tipoSituacaoInscricao,
		String situacaoInscricao, String tipoCredito, String dataInscricao, String indicadorAjuizado, BigDecimal valor) {
}
